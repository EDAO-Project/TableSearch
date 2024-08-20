package com.thetis.loader.progressive.recorder;

import java.util.List;

public class MeanRecorder extends QueryRecorder
{
    private double meanScore = 0;

    public MeanRecorder(List<Double> scores)
    {
        scores.forEach(score -> this.meanScore += score);
        this.meanScore /= scores.size();
    }

    @Override
    protected double abstractBoost(String id)
    {
        if (!super.scores.containsKey(id))
        {
            return 1.0;
        }

        return 1 + super.scores.get(id).get(super.scores.get(id).size() - 1) - this.meanScore;
    }
}
