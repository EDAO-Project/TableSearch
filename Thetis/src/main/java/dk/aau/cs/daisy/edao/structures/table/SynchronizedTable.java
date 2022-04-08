package dk.aau.cs.daisy.edao.structures.table;

import java.util.List;

public class SynchronizedTable<T> extends SimpleTable<T> implements Table<T>
{
    public SynchronizedTable(String ... columnLabels)
    {
        super(columnLabels);
    }

    public SynchronizedTable(List<List<T>> table, String ... columnLabels)
    {
        super(table, columnLabels);
    }

    @Override
    public synchronized Row<T> getRow(int index)
    {
        return super.getRow(index);
    }

    @Override
    public synchronized Column<T> getColumn(int index)
    {
        return super.getColumn(index);
    }

    @Override
    public synchronized Column<T> getColumn(String label)
    {
        return super.getColumn(label);
    }

    @Override
    public synchronized String[] getColumnLabels()
    {
        return super.getColumnLabels();
    }

    @Override
    public synchronized void addRow(Row<T> row)
    {
        super.addRow(row);
    }

    @Override
    public synchronized int rowCount()
    {
        return super.rowCount();
    }

    // Does not need to be synchronized as table attributes cannot be modified
    @Override
    public int columnCount()
    {
        return super.columnCount();
    }

    @Override
    public String toString()
    {
        return toStr();
    }
}
