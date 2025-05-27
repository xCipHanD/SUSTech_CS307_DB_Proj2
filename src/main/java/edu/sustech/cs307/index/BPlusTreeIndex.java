package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap; // Using TreeMap to simulate ordered index behavior for now

// Placeholder for a more complete B+ Tree Index implementation
public class BPlusTreeIndex implements Index {
    private BPlusTreeNode root;
    private int degree;
    private String tableName;
    private String columnName;
    private Map<Value, ArrayList<RID>> map; // Temporary stand-in for B+Tree structure

    public BPlusTreeIndex(String tableName, String columnName, int degree) {
        this.root = new BPlusTreeNode(true, degree); // Pass degree to root node
        this.degree = degree > 0 ? degree : BPlusTreeNode.DEFAULT_DEGREE;
        this.tableName = tableName;
        this.columnName = columnName;
        this.map = new TreeMap<>((v1, v2) -> {
            try {
                return ValueComparer.compare(v1, v2);
            } catch (DBException e) {
                // Wrap the checked DBException in an unchecked exception
                throw new RuntimeException("Comparison failed in BPlusTreeIndex TreeMap: " + e.getMessage(), e);
            }
        });
    }

    public BPlusTreeNode getRoot() {
        return root;
    }

    /**
     * Searches for RIDs associated with a given key.
     * This is a simplified search that directly uses the TreeMap.
     * A full B+ Tree search would traverse from the root down to a leaf.
     * 
     * @param key The key to search for.
     * @return A list of RIDs, or an empty list if the key is not found.
     */
    public List<RID> search(Value key) throws DBException {
        // Simulate B+ Tree search using the TreeMap
        // return map.getOrDefault(key, new ArrayList<>()); // Old TreeMap based search
        if (root == null) {
            return new ArrayList<>();
        }
        return searchRecursive(root, key);
    }

    private List<RID> searchRecursive(BPlusTreeNode node, Value key) throws DBException {
        if (node.isLeaf) {
            List<RID> resultRIDs = new ArrayList<>();
            int keyIndex = node.findKeyIndex(key); // Uses binary search
            if (keyIndex >= 0) {
                // Key found. Assuming one RID per key entry for now.
                // If multiple RIDs per key value are possible and stored as a list at
                // node.rids.get(keyIndex),
                // this part would need to handle that.
                resultRIDs.add(node.rids.get(keyIndex));
                // Add logic here if duplicate keys (identical Value objects) can map to
                // distinct RIDs
                // and are stored sequentially in the leaf. E.g., scan left and right from
                // keyIndex.
            }
            return resultRIDs;
        } else {
            // Internal node: find the child to traverse
            int childPointerIndex = node.findChildPointerIndex(key);
            if (childPointerIndex >= node.children.size() || childPointerIndex < 0) {
                // Should not happen in a well-formed tree if key is within tree's range
                return new ArrayList<>();
            }
            return searchRecursive(node.children.get(childPointerIndex), key);
        }
    }

    /**
     * Searches for RIDs within a given key range.
     * This is a simplified search that directly uses the TreeMap's subMap
     * functionality.
     * 
     * @param startKey The start of the range (inclusive).
     * @param endKey   The end of the range (inclusive).
     * @return A list of RIDs.
     */
    public List<RID> searchRange(Value startKey, Value endKey) throws DBException {
        // return searchRangeRecursive(root, startKey, endKey, true, true); // Old call
        return searchRangeIterative(startKey, endKey, true, true); // Using new iterative method
    }

    // Insert and Delete methods would manipulate the B+ Tree structure (root,
    // nodes)
    // These are complex and involve splitting/merging nodes.

    @Override
    public void insert(Value key, RID rid) throws DBException {
        // Simplified insert for TreeMap simulation - needs to be replaced with B+ Tree
        // logic
        // map.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
        if (root == null) {
            root = new BPlusTreeNode(true, degree);
        }
        insertRecursive(root, key, rid, null); // Parent is initially null
    }

    private void insertRecursive(BPlusTreeNode node, Value key, RID rid, BPlusTreeNode parent) throws DBException {
        if (node.isLeaf) {
            if (!node.isFull()) {
                node.insertIntoLeaf(key, rid);
            } else {
                // Handle leaf node split
                BPlusTreeNode newLeaf = splitLeafNode(node, key, rid);
                Value newKeyToParent = newLeaf.keys.get(0);

                if (parent == null) {
                    // Root was split, create new root
                    BPlusTreeNode newRoot = new BPlusTreeNode(false, degree);
                    newRoot.keys.add(newKeyToParent);
                    newRoot.children.add(node);
                    newRoot.children.add(newLeaf);
                    this.root = newRoot;
                } else {
                    // Propagate key to parent
                    // This part needs to handle parent splitting as well, if parent is full
                    // For now, assuming parent has space or we need another method for parent
                    // insertion/split
                    insertIntoParent(parent, newKeyToParent, node, newLeaf);
                }
            }
        } else {
            // Internal node: find child to traverse
            int childIndex = findChildIndexForKey(node, key);
            BPlusTreeNode child = node.children.get(childIndex);
            insertRecursive(child, key, rid, node); // Pass current node as parent
        }
    }

    // Placeholder for splitting a leaf node. Returns the new sibling node.
    private BPlusTreeNode splitLeafNode(BPlusTreeNode leaf, Value newKey, RID newRid) throws DBException {
        // Create a new leaf node
        BPlusTreeNode newLeaf = new BPlusTreeNode(true, degree);

        // Temporary list to hold all keys and RIDs before splitting
        List<Value> tempKeys = new ArrayList<>(leaf.keys);
        List<RID> tempRids = new ArrayList<>(leaf.rids);

        // Find position for newKey and newRid
        int i = 0;
        while (i < tempKeys.size() && ValueComparer.compare(tempKeys.get(i), newKey) < 0) {
            i++;
        }
        tempKeys.add(i, newKey);
        tempRids.add(i, newRid);

        // Clear original leaf and redistribute
        leaf.keys.clear();
        leaf.rids.clear();

        int midPoint = (degree) / 2; // Split point, keys up to midPoint-1 stay in old leaf

        for (int j = 0; j < midPoint; j++) {
            leaf.keys.add(tempKeys.get(j));
            leaf.rids.add(tempRids.get(j));
        }

        for (int j = midPoint; j < tempKeys.size(); j++) {
            newLeaf.keys.add(tempKeys.get(j));
            newLeaf.rids.add(tempRids.get(j));
        }

        // Link leaf nodes
        newLeaf.nextLeaf = leaf.nextLeaf;
        leaf.nextLeaf = newLeaf;

        return newLeaf;
    }

    // Placeholder for inserting into a parent node after a child split.
    // This needs to handle splitting the parent if it's full.
    private void insertIntoParent(BPlusTreeNode parent, Value key, BPlusTreeNode leftChild, BPlusTreeNode rightChild)
            throws DBException {
        if (!parent.isFull()) {
            // Find position to insert key and new child (rightChild)
            int i = 0;
            while (i < parent.keys.size() && ValueComparer.compare(parent.keys.get(i), key) < 0) {
                i++;
            }
            parent.keys.add(i, key);
            parent.children.add(i + 1, rightChild); // rightChild is to the right of the new key
            // leftChild should already be in parent.children, or needs to be set if it was
            // the one that split
            // This logic might need refinement based on how children are managed during
            // splits.
        } else {
            // Parent is full, split parent
            // This is a complex operation: create new internal node, redistribute keys and
            // children,
            // and then recursively insert the middle key from this split into its parent.
            // For now, this is a TODO.
            throw new DBException(edu.sustech.cs307.exception.ExceptionTypes
                    .InvalidOperation("Parent splitting not yet implemented."));
        }
    }

    // Helper to find which child to traverse to in an internal node for a given
    // key.
    private int findChildIndexForKey(BPlusTreeNode node, Value key) throws DBException {
        if (node.isLeaf) {
            throw new IllegalStateException("Cannot find child index in a leaf node.");
        }
        int i = 0;
        // Find the first key greater than or equal to the search key
        while (i < node.keys.size() && ValueComparer.compare(node.keys.get(i), key) < 0) {
            i++;
        }
        // The child pointer is at this index i
        return i;
    }

    @Override
    public void delete(Value key, RID rid) throws DBException {
        if (root == null) {
            Logger.warn("Attempting to delete from empty B+ Tree");
            return;
        }
        deleteRecursive(root, key, rid, null);
    }

    private void deleteRecursive(BPlusTreeNode node, Value key, RID rid, BPlusTreeNode parent) throws DBException {
        if (node.isLeaf) {
            // Find and remove the key-RID pair from leaf
            int keyIndex = node.findKeyIndex(key);
            if (keyIndex >= 0 && keyIndex < node.rids.size() && node.rids.get(keyIndex).equals(rid)) {
                node.keys.remove(keyIndex);
                node.rids.remove(keyIndex);

                // Check for underflow (simplified - in a full implementation,
                // we'd handle merging/redistribution with siblings)
                if (node.keys.isEmpty() && parent == null) {
                    // Root is empty
                    root = null;
                }
            } else {
                Logger.warn("Key " + key + " with RID " + rid + " not found for deletion in B+ Tree");
            }
        } else {
            // Internal node: find child to traverse
            int childIndex = node.findChildPointerIndex(key);
            if (childIndex >= 0 && childIndex < node.children.size()) {
                deleteRecursive(node.children.get(childIndex), key, rid, node);
            }
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

    // Helper method to get the underlying map for IndexScanOperator (temporary)
    public Map<Value, ArrayList<RID>> getMap() {
        return this.map;
    }

    // Implementing missing methods from Index interface with temporary/placeholder
    // logic

    @Override
    public List<RID> searchRange(Value startKey, Value endKey, boolean startInclusive, boolean endInclusive)
            throws DBException {
        if (root == null) {
            return new ArrayList<>();
        }
        // return searchRangeRecursive(root, startKey, endKey, startInclusive,
        // endInclusive); // Old recursive call
        return searchRangeIterative(startKey, endKey, startInclusive, endInclusive); // Using new iterative method
    }

    // Iterative range search using leaf node traversal
    private List<RID> searchRangeIterative(Value startKey, Value endKey, boolean startInclusive, boolean endInclusive)
            throws DBException {
        List<RID> resultRIDs = new ArrayList<>();
        if (root == null)
            return resultRIDs;

        // 1. Find the starting leaf node
        BPlusTreeNode currentLeaf = findLeafNodeForKey(root, startKey);

        // 2. Scan leaf nodes
        while (currentLeaf != null) {
            for (int i = 0; i < currentLeaf.keys.size(); i++) {
                Value currentKey = currentLeaf.keys.get(i);
                RID currentRid = currentLeaf.rids.get(i);

                // Check if currentKey is past endKey
                int cmpWithEndKey = ValueComparer.compare(currentKey, endKey);
                if (cmpWithEndKey > 0 || (cmpWithEndKey == 0 && !endInclusive)) {
                    return resultRIDs; // All subsequent keys will also be too large
                }

                // Check if currentKey is within startKey boundary
                int cmpWithStartKey = ValueComparer.compare(currentKey, startKey);
                if (cmpWithStartKey > 0 || (cmpWithStartKey == 0 && startInclusive)) {
                    resultRIDs.add(currentRid);
                }
            }
            currentLeaf = currentLeaf.nextLeaf; // Move to the next leaf
        }
        return resultRIDs;
    }

    // Helper to find the leaf node that should contain a given key (or where it
    // would be inserted)
    private BPlusTreeNode findLeafNodeForKey(BPlusTreeNode node, Value key) throws DBException {
        if (node == null)
            return null;
        BPlusTreeNode current = node;
        while (!current.isLeaf) {
            int childIndex = current.findChildPointerIndex(key);
            if (childIndex >= current.children.size() || childIndex < 0) {
                // This indicates an issue or key out of tree bounds, return null or handle
                // error
                // For range scan, if startKey is larger than all keys, it might point beyond
                // last child.
                // If findChildPointerIndex returns keys.size(), it means rightmost child.
                // If it returns an invalid index due to an error, that's a problem.
                // For now, assume valid index or that the last child is chosen if key is very
                // large.
                return null; // Or handle as an error / edge case
            }
            current = current.children.get(childIndex);
        }
        return current;
    }

    // private List<RID> searchRangeRecursive(BPlusTreeNode node, Value startKey,
    // Value endKey, boolean startInclusive, boolean endInclusive) throws
    // DBException { ... }
    // Commenting out the old recursive range search as it was complex and less
    // efficient than iterative scan.

    @Override
    public java.util.Iterator<java.util.Map.Entry<Value, RID>> LessThan(Value value, boolean isEqual) {
        // Placeholder implementation - requires actual B+ Tree logic
        throw new UnsupportedOperationException("LessThan not implemented yet.");
    }

    @Override
    public java.util.Iterator<java.util.Map.Entry<Value, RID>> MoreThan(Value value, boolean isEqual) {
        // Placeholder implementation - requires actual B+ Tree logic
        throw new UnsupportedOperationException("MoreThan not implemented yet.");
    }

    @Override
    public java.util.Iterator<java.util.Map.Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual,
            boolean rightEqual) {
        // Placeholder implementation - requires actual B+ Tree logic
        throw new UnsupportedOperationException("Range not implemented yet.");
    }

    @Override
    public RID EqualTo(Value value) {
        // Placeholder implementation - requires actual B+ Tree logic
        // This method signature (returning single RID) might conflict with search(Value
        // key) returning List<RID>.
        // Consider unifying or clarifying their distinct purposes.
        throw new UnsupportedOperationException("EqualTo not implemented yet.");
    }
}
