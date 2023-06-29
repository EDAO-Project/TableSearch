package dk.aau.cs.daisy.edao.structures;

import com.thetis.structures.table.SynchronizedTable;
import com.thetis.structures.table.Table;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SynchronizedTableTest extends TableTest
{
    private static final String[] ATTRIBUTES = {"attr1", "attr2", "attr3"};

    @Override
    public Table<Integer> setup()
    {
        return new SynchronizedTable<>(ATTRIBUTES);
    }

    protected String[] attributes()
    {
        return ATTRIBUTES;
    }

    @Test
    public void testSynchronization()
    {
        Table<Integer> table = setup();
        List<Thread> threads = new ArrayList<>(20);

        for (int i = 0; i < 10; i++)
        {
            Thread t = new Thread(() -> {
                for (int j = 0; j < 100; j++)
                {
                    table.addRow(new Table.Row<>(1, 1, 1));
                }
            });
            t.start();
            threads.add(t);
        }

        for (int i = 0; i < 10; i++)
        {
            Table.Row<Integer> copy = table.getRow(i);
            Thread t = new Thread(() -> {
                Table.Row<Integer> newRow = new Table.Row<>(copy.get(0) + 1, copy.get(1) + 1, copy.get(2) + 1);
                table.addRow(newRow);
            });
            t.start();
            threads.add(t);
        }

        for (int i = 0; i < threads.size(); i++)
        {
            try
            {
                threads.get(i).join();
            }

            catch (InterruptedException e)
            {
                fail("Failing joining thread");
            }
        }

        for (int i = 0; i < table.rowCount(); i++)
        {
            Table.Row<Integer> r = table.getRow(i);
            assertTrue(r.get(0) == 1 || r.get(0) == 2);
            assertTrue(r.get(1) == 1 || r.get(1) == 2);
            assertTrue(r.get(2) == 1 || r.get(2) == 2);
        }
    }
}
