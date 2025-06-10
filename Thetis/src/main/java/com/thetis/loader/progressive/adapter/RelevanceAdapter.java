package com.thetis.loader.progressive.adapter;

import com.thetis.search.Result;
import com.thetis.structures.Pair;

import java.util.*;

public class RelevanceAdapter implements IndexingAdapter
{
    private Result oldResult, newResult;
    private int levels;

    public RelevanceAdapter(Result oldResult, Result newResult, int priorityLevels)
    {
        this.oldResult = oldResult;
        this.newResult = newResult;
        this.levels = priorityLevels;
    }

    /**
     * The greater the difference, the greater the priority boost
     * Join search-based
     * @return Increments in priority
     */
    @Override
    public List<Pair<String, Double>> newPriorities()
    {
        Map<String, Double> relevanceScores = new HashMap<>();
        Iterator<Pair<String, Double>> resultsIter = this.oldResult.getResults();
        List<Pair<String, Double>> priorityIncrements = new ArrayList<>();

        while (resultsIter.hasNext())
        {
            Pair<String, Double> result = resultsIter.next();
            relevanceScores.put(result.getFirst(), result.getSecond());
        }

        double maxDifference = -1.0;
        resultsIter = this.newResult.getResults();

        while (resultsIter.hasNext())
        {
            Pair<String, Double> result = resultsIter.next();
            double difference = Math.abs(result.getSecond() - relevanceScores.get(result.getFirst()));
            relevanceScores.put(result.getFirst(), difference);

            if (difference > maxDifference)
            {
                maxDifference = difference;
            }
        }

        double scaler = 1 / (maxDifference * levels);

        for (Map.Entry<String, Double> entry : relevanceScores.entrySet())
        {
            double boostFraction = entry.getValue() * this.levels * scaler,
                    priorityBoost = Math.round(this.levels * boostFraction);
            priorityIncrements.add(new Pair<>(entry.getKey(), priorityBoost));
        }

        return priorityIncrements;
    }
}
