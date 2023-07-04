package dk.aau.cs.daisy.edao.structures;

import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import com.thetis.structures.table.Aggregator;
import com.thetis.structures.table.ColumnAggregator;
import com.thetis.structures.table.DynamicTable;
import com.thetis.structures.table.Table;
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
        Entity ent1 = new Entity("uri1", List.of(new Type("t1"), new Type("t2")), List.of()),
                ent2 = new Entity("uri2", List.of(new Type("t2"), new Type("t3"), new Type("t4")), List.of()),
                ent3 = new Entity("uri3", List.of(new Type("t1"), new Type("t3"), new Type("t5")), List.of()),
                ent4 = new Entity("uri4", List.of(new Type("t6")), List.of()),
                ent5 = new Entity("uri5", List.of(new Type("t6"), new Type("t7")), List.of()),
                ent6 = new Entity("uri6", List.of(new Type("t1"), new Type("t4")), List.of());
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
