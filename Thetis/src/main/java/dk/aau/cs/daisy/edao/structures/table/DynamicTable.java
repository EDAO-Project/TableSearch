package dk.aau.cs.daisy.edao.structures.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Synchronized table with varying row lengths
 * @param <T>
 */
public class DynamicTable<T> implements Table<T>
{
    private List<Row<T>> table;
    private List<String> labels;

    public DynamicTable(String ... columnLabels)
    {
        this.labels = List.of(columnLabels);
        this.table = new Vector<>();    // Vector does the synchronization for us
    }

    public DynamicTable(List<List<T>> table, String ... columnLabels)
    {
        this(columnLabels);

        for (List<T> row : table)
        {
            this.table.add(new Row<T>(row));
        }
    }

    public DynamicTable(List<List<T>> table)
    {
        this(table, new String[]{});
    }

    @Override
    public Row<T> getRow(int index)
    {
        return this.table.get(index);
    }

    @Override
    public Column<T> getColumn(int index)
    {
        List<T> elements = new ArrayList<>(this.table.size());

        for (Row<T> row : this.table)
        {
            if (row.size() > index)
                elements.add(row.get(index));
        }

        return new Column<T>(this.labels.get(index), elements);
    }

    @Override
    public Column<T> getColumn(String label)
    {
        if (!this.labels.contains(label))
            throw new IllegalArgumentException("Column label does not exist");

        return getColumn(this.labels.indexOf(label));
    }

    @Override
    public String[] getColumnLabels()
    {
        String[] labels = new String[this.labels.size()];

        for (int i = 0; i < this.labels.size(); i++)
        {
            labels[i] = this.labels.get(i);
        }

        return labels;
    }

    @Override
    public void addRow(Row<T> row)
    {
        this.table.add(row);
    }

    @Override
    public int rowCount()
    {
        return this.table.size();
    }

    /**
     * This does not represent number of row elements since that can be dynamic
     * @return NUmber of attributes given under object construction
     */
    @Override
    public int columnCount()
    {
        return this.labels.size();
    }

    @Override
    public String toString()
    {
        return toStr();
    }
}
