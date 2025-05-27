package edu.sustech.cs307.index;

import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer; // Import ValueComparer
import edu.sustech.cs307.exception.DBException; // Import DBException
import edu.sustech.cs307.record.RID;
import java.util.List;
import java.util.ArrayList;

/**
 * Represents a node in the B+ Tree.
 * This is a simplified structure. A full implementation would handle splitting,
 * merging,
 * redistribution, and manage keys and children/data pointers more dynamically.
 */
public class BPlusTreeNode {
    static final int DEFAULT_DEGREE = 4; // Example degree (fanout)
    int degree;

    boolean isLeaf;
    List<Value> keys;
    List<BPlusTreeNode> children; // For internal nodes
    List<RID> rids; // For leaf nodes (simplified: one RID per key)
    // In a more complete implementation, leaf nodes might store lists of RIDs or
    // pointers to data records.
    BPlusTreeNode nextLeaf; // For leaf nodes, to link to the next leaf node

    public BPlusTreeNode(boolean isLeaf) {
        this(isLeaf, DEFAULT_DEGREE);
    }

    public BPlusTreeNode(boolean isLeaf, int degree) {
        this.isLeaf = isLeaf;
        this.degree = degree;
        this.keys = new ArrayList<>();
        if (isLeaf) {
            this.rids = new ArrayList<>();
            this.children = null; // Leaf nodes don't have children nodes in the B+ tree sense
        } else {
            this.children = new ArrayList<>();
            this.rids = null;
        }
        this.nextLeaf = null;
    }

    public boolean isFull() {
        return keys.size() >= degree - 1; // Max keys is degree - 1
    }

    // Inserts a key-RID pair into a leaf node. Assumes node is not full.
    // Returns the index where key was inserted.
    public int insertIntoLeaf(Value key, RID rid) throws DBException {
        if (!isLeaf) {
            throw new IllegalStateException("Cannot insert RID into an internal node.");
        }
        int i = 0;
        while (i < keys.size() && ValueComparer.compare(keys.get(i), key) < 0) {
            i++;
        }
        keys.add(i, key);
        rids.add(i, rid);
        return i;
    }

    // Inserts a key and a child pointer into an internal node. Assumes node is not
    // full.
    // This is typically used after a child splits.
    public void insertIntoInternal(Value key, BPlusTreeNode leftChild, BPlusTreeNode rightChild) throws DBException {
        if (isLeaf) {
            throw new IllegalStateException("Cannot insert child node into a leaf node.");
        }
        int i = 0;
        while (i < keys.size() && ValueComparer.compare(keys.get(i), key) < 0) {
            i++;
        }
        keys.add(i, key);
        // children list has one more element than keys list
        // if key is inserted at index i, new right child goes to children[i+1]
        // existing children[i] becomes the new leftChild (if it was the one that split)
        // or it remains if the split happened to its right.
        // For simplicity, assuming leftChild is children.get(i) and we add rightChild
        // at i+1
        children.set(i, leftChild); // This might need adjustment based on split logic
        children.add(i + 1, rightChild);
    }

    // Example: A simple search method within a node (very basic)
    // Now uses binary search. Returns index of key if found, or -(insertion point)
    // - 1 if not found.
    public int findKeyIndex(Value key) throws DBException {
        int low = 0;
        int high = keys.size() - 1;
        int mid;
        while (low <= high) {
            mid = low + (high - low) / 2;
            Value midVal = keys.get(mid);
            int cmp = ValueComparer.compare(midVal, key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1); // key not found, low is the insertion point
    }

    /**
     * Finds the index of the child pointer to follow for a given key in an internal
     * node.
     * Keys in internal nodes act as separators.
     * If key < keys[0], follow children[0].
     * If keys[i] <= key < keys[i+1], follow children[i+1].
     * If key >= keys[keys.size()-1], follow children[keys.size()].
     * 
     * @param key The key to search for.
     * @return The index of the child in the children list.
     * @throws DBException If comparison fails.
     */
    public int findChildPointerIndex(Value key) throws DBException {
        if (isLeaf) {
            throw new IllegalStateException("Cannot find child pointer index in a leaf node.");
        }
        // Binary search for the first key greater than or equal to the search key
        int low = 0;
        int high = keys.size() - 1;
        int R_idx = keys.size(); // Default to the rightmost child pointer

        while (low <= high) {
            int mid = low + (high - low) / 2;
            int cmp = ValueComparer.compare(key, keys.get(mid));

            if (cmp < 0) { // key < keys[mid]
                R_idx = mid;
                high = mid - 1;
            } else { // key >= keys[mid]
                low = mid + 1;
            }
        }
        return R_idx; // This index corresponds to the child pointer to follow
    }

    // Further methods for B+ tree operations (insert, split, etc.) are needed.
}
