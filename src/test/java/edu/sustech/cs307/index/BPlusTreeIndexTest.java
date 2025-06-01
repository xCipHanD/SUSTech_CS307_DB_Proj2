package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;

import java.util.List;
import java.util.Iterator;
import java.util.Map;

/**
 * Test class for B+ Tree Index functionality
 */
public class BPlusTreeIndexTest {

    public static void main(String[] args) {
        try {
            testBasicOperations();
            testRangeSearch();
            testIterators();
            testTreeValidation();
            System.out.println("All B+ Tree tests passed!");
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testBasicOperations() throws DBException {
        System.out.println("Testing basic operations...");

        BPlusTreeIndex index = new BPlusTreeIndex("test_table", "test_column", 4);

        // Test insertions - using Long values instead of int
        index.insert(new Value(10L), new RID(1, 1));
        index.insert(new Value(20L), new RID(1, 2));
        index.insert(new Value(5L), new RID(1, 3));
        index.insert(new Value(15L), new RID(1, 4));
        index.insert(new Value(25L), new RID(1, 5));

        // Test search
        List<RID> results = index.search(new Value(15L));
        assert !results.isEmpty() : "Should find inserted value";
        assert results.get(0).equals(new RID(1, 4)) : "Should return correct RID";

        // Test search for non-existent key
        results = index.search(new Value(100L));
        assert results.isEmpty() : "Should not find non-existent value";

        // Test deletion
        index.delete(new Value(15L), new RID(1, 4));
        results = index.search(new Value(15L));
        assert results.isEmpty() : "Should not find deleted value";

        System.out.println("Basic operations test passed!");
    }

    private static void testRangeSearch() throws DBException {
        System.out.println("Testing range search...");

        BPlusTreeIndex index = new BPlusTreeIndex("test_table", "test_column", 4);

        // Insert test data
        for (int i = 0; i < 20; i++) {
            index.insert(new Value((long) i), new RID(1, i));
        }

        // Test range search
        List<RID> results = index.searchRange(new Value(5L), new Value(15L), true, true);
        assert results.size() == 11 : "Should find 11 values in range [5,15], but found " + results.size();

        // Test exclusive range
        results = index.searchRange(new Value(5L), new Value(15L), false, false);
        assert results.size() == 9 : "Should find 9 values in range (5,15), but found " + results.size();

        System.out.println("Range search test passed!");
    }

    private static void testIterators() throws DBException {
        System.out.println("Testing iterators...");

        BPlusTreeIndex index = new BPlusTreeIndex("test_table", "test_column", 4);

        // Insert test data
        for (int i = 0; i < 10; i++) {
            index.insert(new Value((long) i), new RID(1, i));
        }

        // Test LessThan iterator
        Iterator<Map.Entry<Value, RID>> iter = index.LessThan(new Value(5L), false);
        int count = 0;
        while (iter.hasNext()) {
            Map.Entry<Value, RID> entry = iter.next();
            Long keyValue = (Long) entry.getKey().value;
            assert keyValue < 5 : "Iterator should only return values < 5, but got " + keyValue;
            count++;
        }
        assert count == 5 : "Should find 5 values less than 5, but found " + count;

        // Test MoreThan iterator
        iter = index.MoreThan(new Value(5L), false);
        count = 0;
        while (iter.hasNext()) {
            Map.Entry<Value, RID> entry = iter.next();
            Long keyValue = (Long) entry.getKey().value;
            assert keyValue > 5 : "Iterator should only return values > 5, but got " + keyValue;
            count++;
        }
        assert count == 4 : "Should find 4 values greater than 5, but found " + count;

        System.out.println("Iterator test passed!");
    }

    private static void testTreeValidation() throws DBException {
        System.out.println("Testing tree validation...");

        // Use a larger degree to get better tree structure
        BPlusTreeIndex index = new BPlusTreeIndex("test_table", "test_column", 5);

        // Insert enough data to cause splits
        for (int i = 0; i < 20; i++) {
            index.insert(new Value((long) i), new RID(1, i));
            assert index.validateTree() : "Tree should be valid after insertion " + i;
        }

        // Print tree structure for debugging
        System.out.println("Tree structure after insertions (degree=5):");
        index.printTree();

        // Delete some values
        for (int i = 0; i < 10; i += 2) {
            index.delete(new Value((long) i), new RID(1, i));
            assert index.validateTree() : "Tree should be valid after deletion " + i;
        }

        System.out.println("Tree validation test passed!");
    }
}