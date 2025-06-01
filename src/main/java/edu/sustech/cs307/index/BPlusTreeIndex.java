package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.AbstractMap;

public class BPlusTreeIndex implements Index {
    private BPlusTreeNode root;
    private final int degree;
    private final String tableName;
    private final String columnName;

    public BPlusTreeIndex(String tableName, String columnName, int degree) {
        this.degree = degree > 0 ? degree : BPlusTreeNode.DEFAULT_DEGREE;
        this.tableName = tableName;
        this.columnName = columnName;
        this.root = null; // Start with empty tree
    }

    public BPlusTreeNode getRoot() {
        return root;
    }

    @Override
    public List<RID> search(Value key) throws DBException {
        if (root == null) {
            return new ArrayList<>();
        }
        return searchRecursive(root, key);
    }

    private List<RID> searchRecursive(BPlusTreeNode node, Value key) throws DBException {
        if (node.isLeaf) {
            List<RID> resultRIDs = new ArrayList<>();
            int keyIndex = node.findKeyIndex(key);
            if (keyIndex >= 0) {
                // Found exact match
                resultRIDs.add(node.rids.get(keyIndex));

                // Check for duplicate keys (same value, different RIDs)
                // Scan left for duplicates
                for (int i = keyIndex - 1; i >= 0; i--) {
                    if (ValueComparer.compare(node.keys.get(i), key) == 0) {
                        resultRIDs.add(0, node.rids.get(i)); // Add at beginning to maintain order
                    } else {
                        break;
                    }
                }

                // Scan right for duplicates
                for (int i = keyIndex + 1; i < node.keys.size(); i++) {
                    if (ValueComparer.compare(node.keys.get(i), key) == 0) {
                        resultRIDs.add(node.rids.get(i));
                    } else {
                        break;
                    }
                }
            }
            return resultRIDs;
        } else {
            // Internal node: find the child to traverse
            int childPointerIndex = node.findChildPointerIndex(key);
            if (childPointerIndex >= node.children.size() || childPointerIndex < 0) {
                return new ArrayList<>();
            }
            return searchRecursive(node.children.get(childPointerIndex), key);
        }
    }

    @Override
    public void insert(Value key, RID rid) throws DBException {
        if (root == null) {
            root = new BPlusTreeNode(true, degree);
            root.insertIntoLeaf(key, rid);
            return;
        }

        InsertResult result = insertRecursive(root, key, rid);

        // If root was split, create new root
        if (result.newChild != null) {
            BPlusTreeNode newRoot = new BPlusTreeNode(false, degree);
            newRoot.keys.add(result.newKey);
            newRoot.children.add(root);
            newRoot.children.add(result.newChild);
            this.root = newRoot;
        }
    }

    // Helper class to handle split results
    private static class InsertResult {
        Value newKey;
        BPlusTreeNode newChild;

        InsertResult() {
            this.newKey = null;
            this.newChild = null;
        }

        InsertResult(Value newKey, BPlusTreeNode newChild) {
            this.newKey = newKey;
            this.newChild = newChild;
        }
    }

    private InsertResult insertRecursive(BPlusTreeNode node, Value key, RID rid) throws DBException {
        if (node.isLeaf) {
            if (!node.isFull()) {
                node.insertIntoLeaf(key, rid);
                return new InsertResult(); // No split occurred
            } else {
                // Split leaf node
                return splitLeafNode(node, key, rid);
            }
        } else {
            // Internal node: find child to traverse
            int childIndex = findChildIndexForKey(node, key);
            if (childIndex >= node.children.size()) {
                childIndex = node.children.size() - 1; // Use last child
            }

            BPlusTreeNode child = node.children.get(childIndex);
            InsertResult childResult = insertRecursive(child, key, rid);

            if (childResult.newChild == null) {
                return new InsertResult(); // No split propagated up
            }

            // Child was split, need to insert new key into this internal node
            if (!node.isFull()) {
                insertIntoInternalNode(node, childResult.newKey, childIndex, childResult.newChild);
                return new InsertResult(); // No split at this level
            } else {
                // This internal node is also full, need to split it
                return splitInternalNode(node, childResult.newKey, childIndex, childResult.newChild);
            }
        }
    }

    private InsertResult splitLeafNode(BPlusTreeNode leaf, Value newKey, RID newRid) throws DBException {
        BPlusTreeNode newLeaf = new BPlusTreeNode(true, degree);

        // Collect all keys and RIDs
        List<Value> allKeys = new ArrayList<>(leaf.keys);
        List<RID> allRids = new ArrayList<>(leaf.rids);

        // Find insertion position
        int insertPos = 0;
        while (insertPos < allKeys.size() && ValueComparer.compare(allKeys.get(insertPos), newKey) < 0) {
            insertPos++;
        }
        allKeys.add(insertPos, newKey);
        allRids.add(insertPos, newRid);

        // Split point - first half stays in original, second half goes to new
        int splitPoint = allKeys.size() / 2;

        // Clear original leaf and fill with first half
        leaf.keys.clear();
        leaf.rids.clear();
        for (int i = 0; i < splitPoint; i++) {
            leaf.keys.add(allKeys.get(i));
            leaf.rids.add(allRids.get(i));
        }

        // Fill new leaf with second half
        for (int i = splitPoint; i < allKeys.size(); i++) {
            newLeaf.keys.add(allKeys.get(i));
            newLeaf.rids.add(allRids.get(i));
        }

        // Update leaf pointers
        newLeaf.nextLeaf = leaf.nextLeaf;
        leaf.nextLeaf = newLeaf;

        // The key to propagate up is the first key of the new leaf
        Value keyToPropagate = newLeaf.keys.get(0);

        return new InsertResult(keyToPropagate, newLeaf);
    }

    private void insertIntoInternalNode(BPlusTreeNode node, Value key, int childIndex, BPlusTreeNode newChild)
            throws DBException {
        // Find insertion position for the key
        int keyPos = 0;
        while (keyPos < node.keys.size() && ValueComparer.compare(node.keys.get(keyPos), key) < 0) {
            keyPos++;
        }

        node.keys.add(keyPos, key);
        // New child goes to the right of the inserted key
        node.children.add(keyPos + 1, newChild);
    }

    private InsertResult splitInternalNode(BPlusTreeNode node, Value newKey, int childIndex, BPlusTreeNode newChild)
            throws DBException {
        BPlusTreeNode newInternal = new BPlusTreeNode(false, degree);

        // Collect all keys and children
        List<Value> allKeys = new ArrayList<>(node.keys);
        List<BPlusTreeNode> allChildren = new ArrayList<>(node.children);

        // Find insertion position for new key
        int keyPos = 0;
        while (keyPos < allKeys.size() && ValueComparer.compare(allKeys.get(keyPos), newKey) < 0) {
            keyPos++;
        }
        allKeys.add(keyPos, newKey);
        allChildren.add(keyPos + 1, newChild);

        // Split point
        int splitPoint = allKeys.size() / 2;
        Value middleKey = allKeys.get(splitPoint);

        // Clear original node and fill with first half
        node.keys.clear();
        node.children.clear();
        for (int i = 0; i < splitPoint; i++) {
            node.keys.add(allKeys.get(i));
        }
        for (int i = 0; i <= splitPoint; i++) {
            node.children.add(allChildren.get(i));
        }

        // Fill new internal node with second half
        for (int i = splitPoint + 1; i < allKeys.size(); i++) {
            newInternal.keys.add(allKeys.get(i));
        }
        for (int i = splitPoint + 1; i < allChildren.size(); i++) {
            newInternal.children.add(allChildren.get(i));
        }

        return new InsertResult(middleKey, newInternal);
    }

    private int findChildIndexForKey(BPlusTreeNode node, Value key) throws DBException {
        if (node.isLeaf) {
            throw new IllegalStateException("Cannot find child index in a leaf node.");
        }
        int i = 0;
        while (i < node.keys.size() && ValueComparer.compare(node.keys.get(i), key) <= 0) {
            i++;
        }
        return i;
    }

    @Override
    public void delete(Value key, RID rid) throws DBException {
        if (root == null) {
            Logger.warn("Attempting to delete from empty B+ Tree");
            return;
        }

        boolean deleted = deleteRecursive(root, key, rid);

        // If root is internal and has no keys, make its only child the new root
        if (!root.isLeaf && root.keys.isEmpty() && !root.children.isEmpty()) {
            root = root.children.get(0);
        }

        // If root is leaf and empty, set to null
        if (root.isLeaf && root.keys.isEmpty()) {
            root = null;
        }

        if (!deleted) {
            Logger.warn("Key {} with RID {} not found for deletion in B+ Tree", key, rid);
        }
    }

    private boolean deleteRecursive(BPlusTreeNode node, Value key, RID rid) throws DBException {
        if (node.isLeaf) {
            // Find and remove the key-RID pair from leaf
            for (int i = 0; i < node.keys.size(); i++) {
                if (ValueComparer.compare(node.keys.get(i), key) == 0 &&
                        node.rids.get(i).equals(rid)) {
                    node.keys.remove(i);
                    node.rids.remove(i);
                    return true;
                }
            }
            return false;
        } else {
            // Internal node: find child to traverse
            int childIndex = node.findChildPointerIndex(key);
            if (childIndex >= node.children.size()) {
                childIndex = node.children.size() - 1;
            }

            return deleteRecursive(node.children.get(childIndex), key, rid);
        }
    }

    @Override
    public List<RID> searchRange(Value startKey, Value endKey, boolean startInclusive, boolean endInclusive)
            throws DBException {
        if (root == null) {
            return new ArrayList<>();
        }
        return searchRangeIterative(startKey, endKey, startInclusive, endInclusive);
    }

    private List<RID> searchRangeIterative(Value startKey, Value endKey, boolean startInclusive, boolean endInclusive)
            throws DBException {
        List<RID> resultRIDs = new ArrayList<>();
        if (root == null)
            return resultRIDs;

        // Find the starting leaf node
        BPlusTreeNode currentLeaf = findLeafNodeForKey(root, startKey);
        if (currentLeaf == null)
            return resultRIDs;

        // Scan leaf nodes
        while (currentLeaf != null) {
            for (int i = 0; i < currentLeaf.keys.size(); i++) {
                Value currentKey = currentLeaf.keys.get(i);
                RID currentRid = currentLeaf.rids.get(i);

                // Check if we've gone past the end key
                int cmpWithEndKey = ValueComparer.compare(currentKey, endKey);
                if (cmpWithEndKey > 0 || (cmpWithEndKey == 0 && !endInclusive)) {
                    return resultRIDs;
                }

                // Check if we're within the start key boundary
                int cmpWithStartKey = ValueComparer.compare(currentKey, startKey);
                if (cmpWithStartKey > 0 || (cmpWithStartKey == 0 && startInclusive)) {
                    resultRIDs.add(currentRid);
                }
            }
            currentLeaf = currentLeaf.nextLeaf;
        }
        return resultRIDs;
    }

    private BPlusTreeNode findLeafNodeForKey(BPlusTreeNode node, Value key) throws DBException {
        if (node == null)
            return null;

        BPlusTreeNode current = node;
        while (!current.isLeaf) {
            int childIndex = current.findChildPointerIndex(key);
            if (childIndex >= current.children.size()) {
                childIndex = current.children.size() - 1;
            }
            current = current.children.get(childIndex);
        }
        return current;
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> LessThan(Value value, boolean isEqual) {
        return new BPlusTreeIterator(this, null, value, true, !isEqual);
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> MoreThan(Value value, boolean isEqual) {
        return new BPlusTreeIterator(this, value, null, !isEqual, true);
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual, boolean rightEqual) {
        return new BPlusTreeIterator(this, low, high, leftEqual, rightEqual);
    }

    @Override
    public RID EqualTo(Value value) {
        try {
            List<RID> results = search(value);
            return results.isEmpty() ? null : results.get(0);
        } catch (DBException e) {
            Logger.error("Error in EqualTo search: " + e.getMessage());
            return null;
        }
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    /**
     * Iterator implementation for B+ Tree traversal
     */
    private static class BPlusTreeIterator implements Iterator<Map.Entry<Value, RID>> {
        private final BPlusTreeIndex index;
        private final Value startKey;
        private final Value endKey;
        private final boolean startInclusive;
        private final boolean endInclusive;

        private BPlusTreeNode currentLeaf;
        private int currentIndex;
        private Map.Entry<Value, RID> nextEntry;
        private boolean hasNextCached;

        public BPlusTreeIterator(BPlusTreeIndex index, Value startKey, Value endKey,
                boolean startInclusive, boolean endInclusive) {
            this.index = index;
            this.startKey = startKey;
            this.endKey = endKey;
            this.startInclusive = startInclusive;
            this.endInclusive = endInclusive;

            this.currentIndex = 0;
            this.hasNextCached = false;

            try {
                initializeIterator();
            } catch (DBException e) {
                Logger.error("Error initializing B+ Tree iterator: " + e.getMessage());
                this.currentLeaf = null;
            }
        }

        private void initializeIterator() throws DBException {
            if (index.root == null) {
                currentLeaf = null;
                return;
            }

            if (startKey == null) {
                // Start from the leftmost leaf
                currentLeaf = findLeftmostLeaf(index.root);
                currentIndex = 0;
            } else {
                // Find the leaf that should contain the start key
                currentLeaf = index.findLeafNodeForKey(index.root, startKey);
                if (currentLeaf != null) {
                    // Find the first position >= startKey
                    currentIndex = 0;
                    while (currentIndex < currentLeaf.keys.size()) {
                        int cmp = ValueComparer.compare(currentLeaf.keys.get(currentIndex), startKey);
                        if (cmp > 0 || (cmp == 0 && startInclusive)) {
                            break;
                        }
                        currentIndex++;
                    }

                    // If we've gone past all keys in this leaf, move to next leaf
                    if (currentIndex >= currentLeaf.keys.size()) {
                        moveToNextLeaf();
                    }
                }
            }
        }

        private BPlusTreeNode findLeftmostLeaf(BPlusTreeNode node) {
            while (!node.isLeaf && !node.children.isEmpty()) {
                node = node.children.get(0);
            }
            return node.isLeaf ? node : null;
        }

        private void moveToNextLeaf() {
            if (currentLeaf != null) {
                currentLeaf = currentLeaf.nextLeaf;
                currentIndex = 0;
            }
        }

        @Override
        public boolean hasNext() {
            if (hasNextCached) {
                return nextEntry != null;
            }

            nextEntry = computeNext();
            hasNextCached = true;
            return nextEntry != null;
        }

        private Map.Entry<Value, RID> computeNext() {
            while (currentLeaf != null) {
                // Check if we have more entries in current leaf
                if (currentIndex < currentLeaf.keys.size()) {
                    Value key = currentLeaf.keys.get(currentIndex);
                    RID rid = currentLeaf.rids.get(currentIndex);

                    // Check if we've exceeded the end boundary
                    if (endKey != null) {
                        try {
                            int cmp = ValueComparer.compare(key, endKey);
                            if (cmp > 0 || (cmp == 0 && !endInclusive)) {
                                return null; // End of range
                            }
                        } catch (DBException e) {
                            Logger.error("Error comparing keys in iterator: " + e.getMessage());
                            return null;
                        }
                    }

                    currentIndex++;
                    return new AbstractMap.SimpleEntry<>(key, rid);
                } else {
                    // Move to next leaf
                    moveToNextLeaf();
                }
            }
            return null;
        }

        @Override
        public Map.Entry<Value, RID> next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }

            Map.Entry<Value, RID> result = nextEntry;
            nextEntry = null;
            hasNextCached = false;
            return result;
        }
    }

    /**
     * Debug method to print tree structure
     */
    public void printTree() {
        if (root == null) {
            System.out.println("Empty tree");
            return;
        }
        printNode(root, 0);
    }

    private void printNode(BPlusTreeNode node, int level) {
        String indent = "  ".repeat(level);
        System.out.print(indent + (node.isLeaf ? "Leaf: " : "Internal: "));

        for (int i = 0; i < node.keys.size(); i++) {
            System.out.print(node.keys.get(i));
            if (node.isLeaf && i < node.rids.size()) {
                System.out.print("(" + node.rids.get(i) + ")");
            }
            if (i < node.keys.size() - 1) {
                System.out.print(", ");
            }
        }
        System.out.println();

        if (!node.isLeaf && node.children != null) {
            for (BPlusTreeNode child : node.children) {
                printNode(child, level + 1);
            }
        }
    }

    /**
     * Validate tree structure integrity
     */
    public boolean validateTree() {
        if (root == null)
            return true;

        try {
            return validateNode(root, null, null) && validateLeafLinks();
        } catch (DBException e) {
            Logger.error("Error validating tree: " + e.getMessage());
            return false;
        }
    }

    private boolean validateNode(BPlusTreeNode node, Value minKey, Value maxKey) throws DBException {
        // Check key count constraints
        if (node != root && node.keys.size() < (degree - 1) / 2) {
            Logger.error("Node has too few keys: " + node.keys.size());
            return false;
        }

        if (node.keys.size() > degree - 1) {
            Logger.error("Node has too many keys: " + node.keys.size());
            return false;
        }

        // Check key ordering within node
        for (int i = 1; i < node.keys.size(); i++) {
            if (ValueComparer.compare(node.keys.get(i - 1), node.keys.get(i)) >= 0) {
                Logger.error("Keys not in ascending order within node");
                return false;
            }
        }

        // Check boundary constraints
        if (minKey != null && !node.keys.isEmpty() &&
                ValueComparer.compare(node.keys.get(0), minKey) < 0) {
            Logger.error("Node violates minimum key constraint");
            return false;
        }

        if (maxKey != null && !node.keys.isEmpty() &&
                ValueComparer.compare(node.keys.get(node.keys.size() - 1), maxKey) > 0) {
            Logger.error("Node violates maximum key constraint");
            return false;
        }

        // Recursively validate children
        if (!node.isLeaf) {
            if (node.children.size() != node.keys.size() + 1) {
                Logger.error("Internal node has incorrect number of children");
                return false;
            }

            for (int i = 0; i < node.children.size(); i++) {
                Value childMinKey = (i == 0) ? minKey : node.keys.get(i - 1);
                Value childMaxKey = (i == node.keys.size()) ? maxKey : node.keys.get(i);

                if (!validateNode(node.children.get(i), childMinKey, childMaxKey)) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean validateLeafLinks() {
        if (root == null)
            return true;

        // Find leftmost leaf
        BPlusTreeNode leftmost = findLeftmostLeaf(root);
        if (leftmost == null)
            return true;

        // Traverse all leaves and check ordering
        BPlusTreeNode current = leftmost;
        Value lastKey = null;

        while (current != null) {
            if (!current.isLeaf) {
                Logger.error("Non-leaf node found in leaf chain");
                return false;
            }

            for (Value key : current.keys) {
                if (lastKey != null) {
                    try {
                        if (ValueComparer.compare(lastKey, key) > 0) {
                            Logger.error("Keys not in order across leaf nodes");
                            return false;
                        }
                    } catch (DBException e) {
                        Logger.error("Error comparing keys in leaf validation: " + e.getMessage());
                        return false;
                    }
                }
                lastKey = key;
            }

            current = current.nextLeaf;
        }

        return true;
    }

    private BPlusTreeNode findLeftmostLeaf(BPlusTreeNode node) {
        while (!node.isLeaf && !node.children.isEmpty()) {
            node = node.children.get(0);
        }
        return node.isLeaf ? node : null;
    }
}
