package dk.aau.cs.daisy.edao.similarity;

/**
 * Base class for generic similarity measurement
 */
public abstract class SimilarityBase implements Similarity
{
    @Override
    public double similarity()
    {
        return performMeasurement();
    }

    protected abstract double performMeasurement();
}
