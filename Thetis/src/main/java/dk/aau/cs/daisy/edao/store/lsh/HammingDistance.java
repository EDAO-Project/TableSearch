package dk.aau.cs.daisy.edao.store.lsh;

import java.util.List;

public class HammingDistance<E> extends Distance
{
    public HammingDistance(List<E> vector1, List<E> vector2)
    {
        super(vector1, vector2);

        if (vector1.size() != vector2.size())
        {
            throw new IllegalArgumentException("The vectors are not of the same dimension");
        }
    }

    @Override
    protected double measureDistance()
    {
        int mismatches = 0;
        int dimension = getVec1().size();

        for (int i = 0; i < dimension; i++)
        {
            mismatches += getVec1().get(i).equals(getVec2().get(i)) ? 0 : 1;
        }

        return mismatches;
    }
}
