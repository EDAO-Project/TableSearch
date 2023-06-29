package com.thetis.similarity;

import java.util.Iterator;
import java.util.List;

/**
 * Cosine similarity between two numeric vectors
 */
public class CosineSimilarity implements Similarity
{
    private List<Double> l1, l2;

    private CosineSimilarity(List<Double> l1, List<Double> l2)
    {
        this.l1 = l1;
        this.l2 = l2;
    }

    public static CosineSimilarity make(List<Double> l1, List<Double> l2)
    {
        return new CosineSimilarity(l1, l2);
    }

    // TODO: Maybe throw exception if the lists are of unequal lengths
    @Override
    public double similarity()
    {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        Iterator<Double> l1Iter = this.l1.iterator(), l2Iter = this.l2.iterator();

        while (l1Iter.hasNext() && l2Iter.hasNext())
        {
            double l1Next = l1Iter.next(), l2Next = l2Iter.next();
            dotProduct += l1Next * l2Next;
            normA += Math.pow(l1Next, 2);
            normB += Math.pow(l2Next, 2);
        }

        if (normA == 0 || normB == 0)
            return 0;

        double cosineSimilarity = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        return cosineSimilarity <= -1.0 ? -1.0 : Math.min(cosineSimilarity, 1.0);
    }
}
