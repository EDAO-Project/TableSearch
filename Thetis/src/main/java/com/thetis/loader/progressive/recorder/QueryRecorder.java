package com.thetis.loader.progressive.recorder;

import com.thetis.structures.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Records information about query results that can be used to compute the alpha variable for boosting queried table priorities
 */
public abstract class QueryRecorder
{
    protected final Map<String, Integer> frequencies = new HashMap<>();
    protected final Map<String, List<Double>> scores = new HashMap<>();

    public void record(Pair<String, Double> result)
    {
        if (!this.frequencies.containsKey(result.getFirst()))
        {
            this.frequencies.put(result.getFirst(), 0);
            this.scores.put(result.getFirst(), new ArrayList<>());
        }

        this.frequencies.replace(result.getFirst(), this.frequencies.get(result.getFirst()) + 1);
        this.scores.get(result.getFirst()).add(result.getSecond());
    }

    public void record(List<Pair<String, Double>> results)
    {
        results.forEach(this::record);
    }

    protected abstract double abstractBoost(String id);

    public double boost(String id)
    {
        return abstractBoost(id);
    }
}
