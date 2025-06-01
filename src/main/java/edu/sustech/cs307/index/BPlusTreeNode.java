package edu.sustech.cs307.index;

import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import java.util.List;
import java.util.ArrayList;

/**
 * Represents a node in the B+ Tree.
 * Enhanced implementation with proper splitting, merging, and key management.
 */
public class BPlusTreeNode {
    static final int DEFAULT_DEGREE = 4; // Example degree (fanout)
    final int degree;

    boolean isLeaf;
    List<Value> keys;
    List<BPlusTreeNode> children; // For internal nodes
    List<RID> rids; // For leaf nodes
    BPlusTreeNode nextLeaf; // For leaf nodes, to link to the next leaf node
    BPlusTreeNode parent; // Reference to parent node for easier tree operations

    public BPlusTreeNode(boolean isLeaf) {
        this(isLeaf, DEFAULT_DEGREE);
    }

    public BPlusTreeNode(boolean isLeaf, int degree) {
        this.isLeaf = isLeaf;
        this.degree = degree;
        this.keys = new ArrayList<>();
        this.parent = null;

        if (isLeaf) {
            this.rids = new ArrayList<>();
            this.children = null;
        } else {
            this.children = new ArrayList<>();
            this.rids = null;
        }
        this.nextLeaf = null;
    }

    public boolean isFull() {
        return keys.size() >= degree - 1; // Max keys is degree - 1
    }

    public boolean isUnderflow() {
        // Root can have fewer keys, minimum for non-root nodes
        int minKeys = Math.max(1, (degree - 1) / 2);
        return keys.size() < minKeys;
    }

    public boolean canLendKey() {
        // Can lend if has more than minimum required keys
        int minKeys = Math.max(1, (degree - 1) / 2);
        return keys.size() > minKeys;
    }

    /**
     * Inserts a key-RID pair into a leaf node in sorted order.
     * Assumes node is not full.
     */
    public int insertIntoLeaf(Value key, RID rid) throws DBException {
        if (!isLeaf) {
            throw new IllegalStateException("Cannot insert RID into an internal node.");
        }

        int insertPos = findInsertionPosition(key);
        keys.add(insertPos, key);
        rids.add(insertPos, rid);
        return insertPos;
    }

    /**
     * Finds the position where a key should be inserted to maintain sorted order.
     */
    private int findInsertionPosition(Value key) throws DBException {
        int pos = 0;
        while (pos < keys.size() && ValueComparer.compare(keys.get(pos), key) < 0) {
            pos++;
        }
        return pos;
    }

    /**
     * Inserts a key and updates children pointers in an internal node.
     * Used after child node splits.
     */
    public void insertIntoInternal(Value key, BPlusTreeNode newRightChild) throws DBException {
        if (isLeaf) {
            throw new IllegalStateException("Cannot insert child node into a leaf node.");
        }

        int insertPos = findInsertionPosition(key);
        keys.add(insertPos, key);
        children.add(insertPos + 1, newRightChild);
        newRightChild.parent = this;
    }

    /**
     * Binary search for exact key match.
     * Returns index if found, or -(insertion point) - 1 if not found.
     */
    public int findKeyIndex(Value key) throws DBException {
        int low = 0;
        int high = keys.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            int cmp = ValueComparer.compare(keys.get(mid), key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // Found exact match
            }
        }
        return -(low + 1); // Not found, return insertion point
    }

    /**
     * Finds the index of the child pointer to follow for a given key in an internal
     * node.
     * For B+ trees: if key <= separator, go left; if key > separator, go right.
     */
    public int findChildPointerIndex(Value key) throws DBException {
        if (isLeaf) {
            throw new IllegalStateException("Cannot find child pointer index in a leaf node.");
        }

        int childIndex = 0;
        for (int i = 0; i < keys.size(); i++) {
            if (ValueComparer.compare(key, keys.get(i)) < 0) {
                break;
            }
            childIndex++;
        }

        // Ensure we don't go out of bounds
        return Math.min(childIndex, children.size() - 1);
    }

    /**
     * Removes a key-RID pair from a leaf node.
     */
    public boolean removeFromLeaf(Value key, RID rid) throws DBException {
        if (!isLeaf) {
            throw new IllegalStateException("Cannot remove RID from an internal node.");
        }

        for (int i = 0; i < keys.size(); i++) {
            if (ValueComparer.compare(keys.get(i), key) == 0 && rids.get(i).equals(rid)) {
                keys.remove(i);
                rids.remove(i);
                return true;
            }
        }
        return false;
    }

    /**
     * Removes a key from an internal node.
     */
    public boolean removeFromInternal(Value key) throws DBException {
        if (isLeaf) {
            throw new IllegalStateException("Cannot remove key from a leaf node using this method.");
        }

        int keyIndex = findKeyIndex(key);
        if (keyIndex >= 0) {
            keys.remove(keyIndex);
            // Note: In a full implementation, we'd also need to handle child pointer
            // removal
            // This is simplified for now
            return true;
        }
        return false;
    }

    /**
     * Gets the minimum key in the subtree rooted at this node.
     */
    public Value getMinKey() {
        if (keys.isEmpty()) {
            return null;
        }

        if (isLeaf) {
            return keys.get(0);
        } else {
            // For internal nodes, get min from leftmost child
            return children.get(0).getMinKey();
        }
    }

    /**
     * Gets the maximum key in the subtree rooted at this node.
     */
    public Value getMaxKey() {
        if (keys.isEmpty()) {
            return null;
        }

        if (isLeaf) {
            return keys.get(keys.size() - 1);
        } else {
            // For internal nodes, get max from rightmost child
            return children.get(children.size() - 1).getMaxKey();
        }
    }

    /**
     * Splits this node and returns the new right sibling.
     * Used during insertions when node becomes full.
     */
    public BPlusTreeNode split() throws DBException {
        BPlusTreeNode newNode = new BPlusTreeNode(isLeaf, degree);
        newNode.parent = this.parent;

        int splitPoint = keys.size() / 2;

        if (isLeaf) {
            // For leaf nodes, copy the second half to new node
            for (int i = splitPoint; i < keys.size(); i++) {
                newNode.keys.add(keys.get(i));
                newNode.rids.add(rids.get(i));
            }

            // Remove the second half from current node
            keys.subList(splitPoint, keys.size()).clear();
            rids.subList(splitPoint, rids.size()).clear();

            // Update leaf links
            newNode.nextLeaf = this.nextLeaf;
            this.nextLeaf = newNode;

        } else {
            // For internal nodes, move the second half to new node
            // The middle key goes up to parent
            for (int i = splitPoint + 1; i < keys.size(); i++) {
                newNode.keys.add(keys.get(i));
            }

            for (int i = splitPoint + 1; i < children.size(); i++) {
                newNode.children.add(children.get(i));
                children.get(i).parent = newNode;
            }

            // Remove the second half from current node
            keys.subList(splitPoint, keys.size()).clear();
            children.subList(splitPoint + 1, children.size()).clear();
        }

        return newNode;
    }

    /**
     * Merges this node with its right sibling.
     * Used during deletions when nodes become underfull.
     */
    public void mergeWithRightSibling(BPlusTreeNode rightSibling, Value separatorKey) throws DBException {
        if (isLeaf != rightSibling.isLeaf) {
            throw new IllegalStateException("Cannot merge nodes of different types.");
        }

        if (isLeaf) {
            // For leaf nodes, simply append all keys and RIDs
            keys.addAll(rightSibling.keys);
            rids.addAll(rightSibling.rids);

            // Update leaf links
            this.nextLeaf = rightSibling.nextLeaf;

        } else {
            // For internal nodes, add the separator key first
            keys.add(separatorKey);
            keys.addAll(rightSibling.keys);

            // Move all children and update their parent pointers
            for (BPlusTreeNode child : rightSibling.children) {
                children.add(child);
                child.parent = this;
            }
        }

        // Clear the right sibling
        rightSibling.keys.clear();
        if (rightSibling.isLeaf) {
            rightSibling.rids.clear();
        } else {
            rightSibling.children.clear();
        }
    }

    /**
     * Borrows a key from the left sibling.
     */
    public void borrowFromLeftSibling(BPlusTreeNode leftSibling, Value separatorKey, int separatorIndex)
            throws DBException {
        if (isLeaf != leftSibling.isLeaf) {
            throw new IllegalStateException("Cannot borrow from sibling of different type.");
        }

        if (isLeaf) {
            // Borrow the last key-RID pair from left sibling
            Value borrowedKey = leftSibling.keys.remove(leftSibling.keys.size() - 1);
            RID borrowedRID = leftSibling.rids.remove(leftSibling.rids.size() - 1);

            // Add to beginning of current node
            keys.add(0, borrowedKey);
            rids.add(0, borrowedRID);

            // Update separator in parent
            if (parent != null && separatorIndex < parent.keys.size()) {
                parent.keys.set(separatorIndex, keys.get(0));
            }

        } else {
            // For internal nodes, the process is more complex
            Value borrowedKey = leftSibling.keys.remove(leftSibling.keys.size() - 1);
            BPlusTreeNode borrowedChild = leftSibling.children.remove(leftSibling.children.size() - 1);

            // Insert the separator key at the beginning
            keys.add(0, separatorKey);
            children.add(0, borrowedChild);
            borrowedChild.parent = this;

            // Update separator in parent
            if (parent != null && separatorIndex < parent.keys.size()) {
                parent.keys.set(separatorIndex, borrowedKey);
            }
        }
    }

    /**
     * Borrows a key from the right sibling.
     */
    public void borrowFromRightSibling(BPlusTreeNode rightSibling, Value separatorKey, int separatorIndex)
            throws DBException {
        if (isLeaf != rightSibling.isLeaf) {
            throw new IllegalStateException("Cannot borrow from sibling of different type.");
        }

        if (isLeaf) {
            // Borrow the first key-RID pair from right sibling
            Value borrowedKey = rightSibling.keys.remove(0);
            RID borrowedRID = rightSibling.rids.remove(0);

            // Add to end of current node
            keys.add(borrowedKey);
            rids.add(borrowedRID);

            // Update separator in parent
            if (parent != null && separatorIndex < parent.keys.size()) {
                parent.keys.set(separatorIndex, rightSibling.keys.get(0));
            }

        } else {
            // For internal nodes
            Value borrowedKey = rightSibling.keys.remove(0);
            BPlusTreeNode borrowedChild = rightSibling.children.remove(0);

            // Add the separator key at the end
            keys.add(separatorKey);
            children.add(borrowedChild);
            borrowedChild.parent = this;

            // Update separator in parent
            if (parent != null && separatorIndex < parent.keys.size()) {
                parent.keys.set(separatorIndex, borrowedKey);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(isLeaf ? "Leaf[" : "Internal[");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0)
                sb.append(", ");
            sb.append(keys.get(i));
        }
        sb.append("]");
        return sb.toString();
    }
}
