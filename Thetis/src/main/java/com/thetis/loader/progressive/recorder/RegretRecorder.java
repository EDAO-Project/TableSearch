package com.thetis.loader.progressive.recorder;

public class RegretRecorder extends QueryRecorder
{
    @Override
    protected double abstractBoost(String id)
    {
        if (!super.scores.containsKey(id))
        {
            return 1.0;
        }

        double current = super.scores.get(id).get(super.scores.get(id).size() - 1),
                avgRegret = super.scores.get(id).subList(0, super.scores.get(id).size() - 1).stream()
                        .mapToDouble(d -> 1 - d)
                        .average()
                        .orElse(1.0);

        return 1 / (1 + Math.abs(current - avgRegret));
    }
}
