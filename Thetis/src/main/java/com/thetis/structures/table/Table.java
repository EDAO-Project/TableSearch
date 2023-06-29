package com.thetis.structures.table;

import java.util.Iterator;
import java.util.List;

public interface Table<T>
{
    Row<T> getRow(int index);
    Column<T> getColumn(int index);
    Column<T> getColumn(String label);
    String[] getColumnLabels();
    void addRow(Row<T> row);
    int rowCount();
    int columnCount();

    default String toStr()
    {
        StringBuilder builder = new StringBuilder("[");
        int size = rowCount();

        if (size == 0)
        {
            return "[]";
        }

        for (int i = 0; i < size; i++)
        {
            builder.append(getRow(i)).append(", ");
        }

        builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append("]");
        return builder.toString();
    }

    class Row<E> implements Iterable<E>
    {
        private List<E> row;

        public Row(E ... elements)
        {
            this(List.of(elements));
        }

        public Row(List<E> elements)
        {
            this.row = elements;
        }

        public E get(int index)
        {
            return this.row.get(index);
        }

        public int size()
        {
            return this.row.size();
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Row))
                return false;

            Row other = (Row) o;
            return this.row.equals(other.row);
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder("[");

            if (this.row.isEmpty())
            {
                return "[]";
            }

            for (E e : this.row)
            {
                builder.append(e.toString()).append(", ");
            }

            builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append("]");
            return builder.toString();
        }

        @Override
        public Iterator<E> iterator()
        {
            return this.row.iterator();
        }
    }

    class Column<E> implements Iterable<E>
    {
        private String label;
        private List<E> elements;

        public Column(String label, E ... columnElements)
        {
            this(label, List.of(columnElements));
        }

        public Column(String label, List<E> columnElements)
        {
            this.label = label;
            this.elements = columnElements;
        }

        public String getLabel()
        {
            return this.label;
        }

        public E get(int row)
        {
            return this.elements.get(row);
        }

        public int size()
        {
            return this.elements.size();
        }

        @Override
        public Iterator<E> iterator()
        {
            return this.elements.iterator();
        }
    }
}
