package dk.aau.cs.daisy.edao.similarity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JaccardSimilarity<E> extends CollectionSimilarity<E>
{
    private JaccardSimilarity(Collection<E> l1, Collection<E> l2)
    {
        super(l1, l2);
    }

    public static <E> JaccardSimilarity<E> make(Collection<E> l1, Collection<E> l2)
    {
        return new JaccardSimilarity<E>(l1, l2);
    }

    @Override
    protected double performMeasurement()
    {
        List<E> intersection = intersection(), union = union();

        if (union.isEmpty())
            return 0;

        return (double) intersection.size() / union.size();
    }

    private List<E> intersection()
    {
        List<E> inter = new ArrayList<>(super.l1);
        inter.retainAll(super.l2);
        return inter;
    }

    private List<E> union()
    {
        List<E> union = new ArrayList<>(super.l1);
        union.addAll(super.l2);
        return union;
    }
}
