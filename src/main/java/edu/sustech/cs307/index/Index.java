package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

public interface Index {
    /**
     * Searches for RIDs associated with a given key.
     * 
     * @param key The key to search for.
     * @return A list of RIDs, or an empty list if the key is not found.
     * @throws DBException If an error occurs during the search.
     */
    List<RID> search(Value key) throws DBException;

    /**
     * Searches for RIDs within a given key range.
     * 
     * @param startKey       The start of the range.
     * @param endKey         The end of the range.
     * @param startInclusive True if the startKey is inclusive, false otherwise.
     * @param endInclusive   True if the endKey is inclusive, false otherwise.
     * @return A list of RIDs matching the range.
     * @throws DBException If an error occurs during the search.
     */
    List<RID> searchRange(Value startKey, Value endKey, boolean startInclusive, boolean endInclusive)
            throws DBException;

    RID EqualTo(Value value);

    Iterator<Entry<Value, RID>> LessThan(Value value, boolean isEqual);

    Iterator<Entry<Value, RID>> MoreThan(Value value, boolean isEqual);

    Iterator<Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual, boolean rightEqual);

    /**
     * Inserts a key-RID pair into the index.
     * 
     * @param key The key to insert.
     * @param rid The RID to associate with the key.
     * @throws DBException If an error occurs during insertion.
     */
    void insert(Value key, RID rid) throws DBException;

    /**
     * Deletes a key-RID pair from the index.
     * 
     * @param key The key to delete.
     * @param rid The RID to remove for the given key.
     * @throws DBException If an error occurs during deletion.
     */
    void delete(Value key, RID rid) throws DBException;

    /**
     * Gets the name of the table this index belongs to.
     * 
     * @return The table name.
     */
    String getTableName();

    /**
     * Gets the name of the column this index is on.
     * 
     * @return The column name.
     */
    String getColumnName();
}
