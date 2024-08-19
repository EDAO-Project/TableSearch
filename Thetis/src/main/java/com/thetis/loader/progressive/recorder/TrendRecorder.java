package com.thetis.loader.progressive.recorder;

import java.util.List;

/**
 * Boosts the query based on trend
 * If a query tends to improve its relevance score when queried, then the alpha boost increases, and vice versa
 * The alpha boost is based on the relevance scores themselves
 */
public class TrendRecorder extends QueryRecorder
{
    @Override
    protected double abstractBoost(String id)
    {
        if (!super.scores.containsKey(id))
        {
            return 1.0;
        }

        List<Double> scores = super.scores.get(id);
        int count = scores.size();
        double sum = 0.0;

        for (int i = 1; i < count; i++)
        {
            sum += scores.get(i) - scores.get(i - 1);
        }

        return 1.0 + sum;
    }
}
