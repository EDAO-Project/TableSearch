package dk.aau.cs.daisy.edao.similarity;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class JaccardSimilarity<E> implements Similarity
{
    private Set<E> s1, s2;

    private JaccardSimilarity(Set<E> s1, Set<E> s2)
    {
        this.s1 = s1;
        this.s2 = s2;
    }

    public static <E> JaccardSimilarity<E> make(Set<E> s1, Set<E> s2)
    {
        return new JaccardSimilarity<E>(s1, s2);
    }

    @Override
    public double similarity()
    {
        List<E> intersection = intersection(), union = union();

        if (union.isEmpty())
            return 0;

        return (double) intersection.size() / union.size();
    }

    private List<E> intersection()
    {
        List<E> inter = new ArrayList<>(this.s1);
        inter.retainAll(this.s2);
        return inter;
    }

    private List<E> union()
    {
        List<E> union = new ArrayList<>(this.s1);
        union.addAll(this.s2);
        return union;
    }
}
