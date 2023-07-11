package com.thetis.structures;

import com.thetis.structures.table.Table;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class TableTest
{
    protected abstract Table<Integer> setup();
    protected abstract String[] attributes();

    @Test
    public void addTest()
    {
        Table<Integer> table = setup();
        String[] attributes = attributes();
        Integer[] rowElements = new Integer[attributes().length];

        for (int i = 1; i <= attributes.length; i++)
        {
            rowElements[i - 1] = i;
        }

        table.addRow(new Table.Row<>(rowElements));
        assertEquals(new Table.Row<>(rowElements), table.getRow(0));
    }

    @Test
    public void testGetColumnLabels()
    {
        Table<Integer> table = setup();
        String[] attributes = attributes(), tableColumns = table.getColumnLabels();
        assertEquals(attributes.length, tableColumns.length);

        for (int i = 0; i < attributes.length; i++)
        {
            assertEquals(attributes[i], tableColumns[i]);
        }
    }

    @Test
    public void testGetColumnByIndex()
    {
        Table<Integer> table = setup();
        String[] attributes = attributes();
        Integer[] r1 = new Integer[attributes.length], r2 = new Integer[attributes.length], r3 = new Integer[attributes.length];

        for (int i = 0; i < attributes.length; i++)
        {
            r1[i] = i + 1;
            r2[i] = i + 4;
            r3[i] = i + 7;
        }

        table.addRow(new Table.Row<>(r1));
        table.addRow(new Table.Row<>(r2));
        table.addRow(new Table.Row<>(r3));

        Table.Column<Integer> c = table.getColumn(attributes.length - 1);
        assertEquals(attributes[attributes.length - 1], c.getLabel());
        assertEquals(3, c.size());
        assertEquals(r1[r1.length - 1], c.get(0));
        assertEquals(r2[r2.length - 1], c.get(1));
        assertEquals(r3[r3.length - 1], c.get(2));
    }

    @Test
    public void testGetColumnByLabel()
    {
        Table<Integer> table = setup();
        String[] attributes = attributes();
        Integer[] r1 = new Integer[attributes.length], r2 = new Integer[attributes.length], r3 = new Integer[attributes.length];

        for (int i = 0; i < attributes.length; i++)
        {
            r1[i] = i + 1;
            r2[i] = i + 4;
            r3[i] = i + 7;
        }

        table.addRow(new Table.Row<>(r1));
        table.addRow(new Table.Row<>(r2));
        table.addRow(new Table.Row<>(r3));

        Table.Column<Integer> c = table.getColumn(attributes[attributes.length - 1]);
        assertEquals(attributes[attributes.length - 1], c.getLabel());
        assertEquals(3, c.size());
        assertEquals(r1[r1.length - 1], c.get(0));
        assertEquals(r2[r2.length - 1], c.get(1));
        assertEquals(r3[r3.length - 1], c.get(2));
    }
}
