package dk.aau.cs.daisy.edao.structures.table;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public interface Aggregator<I>
{
    <E> List<E> aggregate(Function<I, E> mapper, Function<Collection<E>, E> aggregator);
}
