package dk.aau.cs.daisy.edao.similarity;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import dk.aau.cs.daisy.edao.structures.Pair;

public class JaccardSimilarity<E extends Comparable<E>> implements Similarity
{
    private Set<E> s1, s2;
    private Set<Pair<E, Double>> weights = null;

    private JaccardSimilarity(Set<E> s1, Set<E> s2)
    {
        this.s1 = s1;
        this.s2 = s2;
    }

    /**
     * @param weights Weights for set elements
     *                First pair element must exist in s1 and/or s2
     */
    private JaccardSimilarity(Set<E> s1, Set<E> s2, Set<Pair<E, Double>> weights)
    {
        this.s1 = s1;
        this.s2 = s2;
        this.weights = weights;
    }

    public static <E extends Comparable<E>> JaccardSimilarity<E> make(Set<E> s1, Set<E> s2)
    {
        return new JaccardSimilarity<E>(s1, s2);
    }

    public static <E extends Comparable<E>> JaccardSimilarity<E> make(Set<E> s1, Set<E> s2, Set<Pair<E, Double>> weights)
    {
        return new JaccardSimilarity<E>(s1, s2, weights);
    }

    @Override
    public double similarity()
    {
        Set<E> intersection = intersection(), union = union();

        if (union.isEmpty())
            return 0;

        // Handle weighted Jaccard similarity where each value in the intersection and union
        // is weighted based on the entityType IDF scores
        if (this.weights != null)
        {
            double numeratorSum = 0.0;
            double denominatorSum = 0.0;

            for (E element : intersection)
            {
                double weight = findWeight(element);

                if (weight != -1)
                    numeratorSum += findWeight(element);
            }

            for (E element : union)
            {
                double weight = findWeight(element);

                if (weight != -1)
                    denominatorSum += findWeight(element);
            }
            
            if (denominatorSum != 0)
                return numeratorSum / denominatorSum;

            else
                return 0.0;
        }

        return (double) intersection.size() / union.size();
    }

    /**
     * This is a temporary hack
     * @return
     */
    private Set<E> intersection()
    {
        Set<E> inter = new TreeSet<>(this.s1);
        Set<String> strings = new HashSet<>();
        this.s1.forEach(e -> strings.add(e.toString()));

        for (E element : this.s2)
        {
            if (strings.contains(element.toString()))
                inter.add(element);
        }

        return inter;
    }

    private Set<E> union()
    {
        Set<E> union = new TreeSet<>(this.s1);
        union.addAll(this.s2);
        return union;
    }

    private double findWeight(E element)
    {
        for (Pair<E, Double> weight : this.weights)
        {
            if (element.compareTo(weight.getFirst()) == 0)
                return weight.getSecond();
        }

        return -1;
    }
}
