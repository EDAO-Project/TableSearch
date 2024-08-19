package com.thetis.loader.progressive.recorder;

public class ThresholdRecorder extends QueryRecorder
{
    private final double threshold;

    public ThresholdRecorder(double threshold)
    {
        this.threshold = threshold;
    }

    @Override
    protected double abstractBoost(String id)
    {
        if (!super.scores.containsKey(id))
        {
            return 0.0;
        }

        double score = super.scores.get(id).get(super.scores.get(id).size() - 1);
        return score >= threshold ? 1.0 : 0.0;
    }
}
