package dk.aau.cs.daisy.edao.structures;

import com.thetis.structures.table.SimpleTable;
import com.thetis.structures.table.Table;
import org.junit.Test;

import static org.junit.Assert.fail;

public class SimpleTableTest extends TableTest
{
    private static final String[] ATTRIBUTES = {"attr1", "attr2", "attr3"};

    @Override
    protected Table<Integer> setup()
    {
        return new SimpleTable<>(ATTRIBUTES);
    }

    @Override
    protected String[] attributes()
    {
        return ATTRIBUTES;
    }

    @Test
    public void testAddWrongRow()
    {
        try
        {
            Integer[] wrongRow = new Integer[ATTRIBUTES.length + 1];
            Table<Integer> table = setup();
            table.addRow(new Table.Row<>(1, 2, 3));

            for (int i = 0; i < wrongRow.length; i++)
            {
                wrongRow[i] = i + 3;
            }

            table.addRow(new Table.Row<>(wrongRow));   // Wrong number of elements
            fail();
        }

        catch (IllegalArgumentException e) {}
    }
}
