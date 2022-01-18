package dk.aau.cs.daisy.edao.similarity;

import java.util.Collection;
import java.util.Iterator;

public class CosineSimilarity extends CollectionSimilarity<Double>
{
    private CosineSimilarity(Collection<Double> l1, Collection<Double> l2)
    {
        super(l1, l2);
    }

    public static CosineSimilarity make(Collection<Double> l1, Collection<Double> l2)
    {
        return new CosineSimilarity(l1, l2);
    }

    @Override
    protected double performMeasurement()
    {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        Iterator<Double> l1Iter = super.l1.iterator(), l2Iter = super.l2.iterator();

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
