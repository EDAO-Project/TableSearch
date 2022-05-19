package dk.aau.cs.daisy.edao.similarity;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import dk.aau.cs.daisy.edao.commands.SearchTables;

public class JaccardSimilarity<E> implements Similarity
{
    private Set<E> s1, s2;
    private boolean weighted = false;

    private JaccardSimilarity(Set<E> s1, Set<E> s2)
    {
        this.s1 = s1;
        this.s2 = s2;
    }

    private JaccardSimilarity(Set<E> s1, Set<E> s2, boolean weighted)
    {
        this.s1 = s1;
        this.s2 = s2;
        this.weighted = weighted;
    }

    public static <E> JaccardSimilarity<E> make(Set<E> s1, Set<E> s2)
    {
        return new JaccardSimilarity<E>(s1, s2);
    }

    public static <E> JaccardSimilarity<E> make(Set<E> s1, Set<E> s2, boolean weighted)
    {
        return new JaccardSimilarity<E>(s1, s2, weighted);
    }

    @Override
    public double similarity()
    {
        HashSet<E> intersection = intersection(), union = union();

        if (union.isEmpty())
            return 0;

        if (this.weighted) {
            // Handle weighted Jaccard similarity where each value in the intersection and union 
            // is weighted based on the entityType IDF scores
            double numerator_sum = 0.0;
            double denominator_sum = 0.0; 
            
            for (E entityType : intersection) {
                numerator_sum += SearchTables.entityTypeToIDF.get(entityType);
            }
            for (E entityType : union) {
                denominator_sum += SearchTables.entityTypeToIDF.get(entityType);
            }
            
            if (denominator_sum != 0) {
                return (double) numerator_sum / denominator_sum;
            }
            else {
                return 0.0;
            }
        }
        else {
            return (double) intersection.size() / union.size();
        }
        
    }

    private HashSet<E> intersection()
    {
        HashSet<E> inter = new HashSet<>(this.s1);
        inter.retainAll(this.s2);
        return inter;
    }

    private HashSet<E> union()
    {
        HashSet<E> union = new HashSet<>(this.s1);
        union.addAll(this.s2);
        return union;
    }
}
