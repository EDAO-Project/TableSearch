package dk.aau.cs.daisy.edao.structures;

import dk.aau.cs.daisy.edao.structures.graph.Entity;
import dk.aau.cs.daisy.edao.structures.graph.Type;
import dk.aau.cs.daisy.edao.structures.table.Aggregator;
import dk.aau.cs.daisy.edao.structures.table.ColumnAggregator;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ColumnAggregatorTest
{
    private Table<Entity> table;

    @Before
    public void setup()
    {
        Entity ent1 = new Entity("uri1", new Type("t1"), new Type("t2")),
                ent2 = new Entity("uri2", new Type("t2"), new Type("t3"), new Type("t4")),
                ent3 = new Entity("uri3", new Type("t1"), new Type("t3"), new Type("t5")),
                ent4 = new Entity("uri4", new Type("t6")),
                ent5 = new Entity("uri5", new Type("t6"), new Type("t7")),
                ent6 = new Entity("uri6", new Type("t1"), new Type("t4"));
        this.table = new DynamicTable<>(List.of(List.of(ent1, ent2), List.of(ent3, ent4), List.of(ent5, ent6)));
    }

    @Test
    public void testAggregator()
    {
        Aggregator<Entity> aggregator = new ColumnAggregator<>(this.table);
        List<Set<Type>> aggregatedColumns =
                aggregator.aggregate(cell -> {
                    Set<Type> types = new HashSet<>();
                    types.addAll(cell.getTypes());
                    return types;
                }, coll -> {
                    Set<Type> columnTypes = new HashSet<>();
                    coll.forEach(columnTypes::addAll);
                    return columnTypes;
                });
        assertEquals(2, aggregatedColumns.size());

        Set<Type> column1 = aggregatedColumns.get(0), column2 = aggregatedColumns.get(1);

        assertTrue(column1.stream().map(Type::getType).collect(Collectors.toSet()).contains("t1"));
        assertTrue(column1.stream().map(Type::getType).collect(Collectors.toSet()).contains("t2"));
        assertTrue(column1.stream().map(Type::getType).collect(Collectors.toSet()).contains("t3"));
        assertTrue(column1.stream().map(Type::getType).collect(Collectors.toSet()).contains("t5"));
        assertTrue(column1.stream().map(Type::getType).collect(Collectors.toSet()).contains("t6"));
        assertTrue(column1.stream().map(Type::getType).collect(Collectors.toSet()).contains("t7"));
        assertFalse(column1.stream().map(Type::getType).collect(Collectors.toSet()).contains("t4"));

        assertTrue(column2.stream().map(Type::getType).collect(Collectors.toSet()).contains("t1"));
        assertTrue(column2.stream().map(Type::getType).collect(Collectors.toSet()).contains("t2"));
        assertTrue(column2.stream().map(Type::getType).collect(Collectors.toSet()).contains("t3"));
        assertTrue(column2.stream().map(Type::getType).collect(Collectors.toSet()).contains("t4"));
        assertTrue(column2.stream().map(Type::getType).collect(Collectors.toSet()).contains("t6"));
        assertFalse(column2.stream().map(Type::getType).collect(Collectors.toSet()).contains("t5"));
        assertFalse(column2.stream().map(Type::getType).collect(Collectors.toSet()).contains("t7"));
    }
}
