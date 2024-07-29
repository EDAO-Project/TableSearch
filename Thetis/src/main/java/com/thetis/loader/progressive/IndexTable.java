package com.thetis.loader.progressive;

import com.thetis.commands.parser.ParsingException;
import com.thetis.commands.parser.TableParser;
import com.thetis.tables.JsonTable;

import java.nio.file.Path;
import java.util.List;

public class IndexTable implements Indexable, Comparable<IndexTable>
{
    private final Path filePath;
    private final String fileId;
    private double priority;
    private JsonTable table = null;
    private int currentRow = 0;
    private final ItemIndexer<List<JsonTable.TableCell>> indexRow;

    public IndexTable(Path filePath, ItemIndexer<List<JsonTable.TableCell>> consumeRow)
    {
        this(filePath, 0, consumeRow);
    }

    public IndexTable(Path filePath, double priority, ItemIndexer<List<JsonTable.TableCell>> consumeRow)
    {
        this.filePath = filePath;
        this.fileId = filePath.toFile().getName();
        this.indexRow = consumeRow;
        this.priority = priority;
    }

    /**
     * Indexes the top row of the table
     * @return The indexed table row or null
     */
    @Override
    public Object index()
    {
        if (this.table == null && !loadTable())
        {
            throw new RuntimeException("Table '" + this.fileId + "' could not be parsed before indexing");
        }

        else if (isIndexed())
        {
            return null;
        }

        List<JsonTable.TableCell> rowToIndex = this.table.rows.get(0);
        this.table.rows.remove(0);
        this.indexRow.index(this.fileId, this.currentRow++, rowToIndex);

        return rowToIndex;
    }

    /**
     * Returns the table to be indexed, but it can be null if the indexing method hasn't been invoked
     * @return Table to index or null if indexing hasn't yet been invoked
     */
    @Override
    public JsonTable getIndexable()
    {
        return this.table;
    }

    private boolean loadTable()
    {
        try
        {
            this.table = TableParser.parse(this.filePath.toFile());
            return true;
        }

        catch (ParsingException e)
        {
            return false;
        }
    }

    /**
     * Checks whether this table is fully indexed
     * @return True if the table is fully indexed
     */
    @Override
    public boolean isIndexed()
    {
        return this.table != null && !this.table.rows.isEmpty();
    }

    public Path getFilePath()
    {
        return this.filePath;
    }

    @Override
    public String getId()
    {
        return this.fileId;
    }

    /**
     * Will return null if it hasn't started being indexed
     * @return The table to index or null if the table has started to be indexed
     */
    public JsonTable getTable()
    {
        return this.table;
    }

    public void increasePriority(int increment)
    {
        this.priority += increment;
    }

    public void decreasePriority(int decrement)
    {
        this.priority -= decrement;
    }

    @Override
    public double getPriority()
    {
        return this.priority;
    }

    /**
     * Set the priority of the currently top row to be indexed
     * @param priority Priority to be assigned to the currently top row to be indexed
     */
    @Override
    public void setPriority(double priority)
    {
        this.priority = priority;
    }

    /**
     * Compare the priority of two tables to index in an descending order
     * @param o Another table to index
     * @return Negative if this object has a higher priority
     */
    @Override
    public int compareTo(IndexTable o)
    {
        if (equals(o))
        {
            return 0;
        }

        int comp = Double.compare(o.priority, this.priority);

        if (comp == 0)
        {
            comp = this.fileId.compareTo(o.fileId);
        }

        return comp;
    }

    /**
     * Checks for equality to another table to be indexed
     * @param o Object to check for equality
     * @return True if the given object is equal to this object
     */
    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof IndexTable other))
        {
            return false;
        }

        else if ((this.table == null && other.table != null) || (other.table == null && this.table != null))
        {
            return false;
        }

        return other.priority == this.priority &&
                other.filePath.equals(this.filePath) &&
                (other.table == null || other.table.equals(this.table));
    }
}
