package dk.aau.cs.daisy.edao.structures.table;

import java.util.ArrayList;
import java.util.List;

public class SimpleTable<T> implements Table<T>
{
    private List<Row<T>> table;
    private List<String> labels;

    public SimpleTable(String ... columnLabels)
    {
        this.labels = List.of(columnLabels);
        this.table = new ArrayList<>();
    }

    public SimpleTable(List<List<T>> table, String ... columnLabels)
    {
        this(columnLabels);

        for (List<T> row : table)
        {
            this.table.add(new Row<T>(row));
        }
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
        if (this.table.isEmpty() || row.size() == this.table.get(this.table.size() - 1).size())
            this.table.add(row);

        else
            throw new IllegalArgumentException("Row size mismatch");
    }

    @Override
    public int rowCount()
    {
        return this.table.size();
    }

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
