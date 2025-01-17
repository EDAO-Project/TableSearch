package com.thetis.loader.progressive;

import com.thetis.search.Result;
import com.thetis.structures.Pair;

import java.util.*;

public class ConsensusResultAdapter extends ResultAdapter implements IndexingAdapter
{
    public ConsensusResultAdapter(Set<Pair<Result, Result>> results)
    {
        super(results);
    }

    @Override
    public List<Pair<String, Double>> newPriorities()
    {
        List<Pair<String, Double>> priorities = new ArrayList<>();
        Map<String, List<Double>> relevanceDifferences = relevanceDifferences();
        Map<String, Double> maxRelevanceDifferences = new HashMap<>();
        relevanceDifferences.forEach((tableIds, diffs) -> maxRelevanceDifferences.put(tableIds, Collections.max(diffs)));

        double relevanceDifferenceSum = relevanceDifferences.values()
                                                            .stream()
                                                            .mapToDouble(coll -> coll.stream().mapToDouble(val -> val).sum())
                                                            .sum();
        double relevanceDifferenceAverage = relevanceDifferenceSum / relevanceDifferences.values()
                                                                                        .stream()
                                                                                        .mapToDouble(List::size)
                                                                                        .sum();
        double theta = relevanceDifferences.values()
                                        .stream()
                                        .mapToDouble(coll -> coll.stream().mapToDouble(diff -> Math.abs(diff - relevanceDifferenceAverage)).sum())
                                        .sum() / relevanceDifferences.values()
                                                                    .stream()
                                                                    .mapToDouble(List::size)
                                                                    .sum();

        for (String tableId : maxRelevanceDifferences.keySet())
        {
            double maxDiff = maxRelevanceDifferences.get(tableId);

            if (maxDiff < theta)
            {
                priorities.add(new Pair<>(tableId, -1.0));
            }

            else
            {
                priorities.add(new Pair<>(tableId, 1.0));
            }
        }

        return priorities;
    }

    private Map<String, List<Double>> relevanceDifferences()
    {
        Map<String, List<Double>> differences = new HashMap<>();

        for (Pair<Result, Result> results : getResults())
        {
            for (Pair<String, Double> oldResult : results.getFirst().getResultSet())
            {
                String tableId = oldResult.getFirst();
                double oldScore = oldResult.getSecond();

                for (Pair<String, Double> newResult : results.getSecond().getResultSet())
                {
                    if (tableId.equals(newResult.getFirst()))
                    {
                        if (!differences.containsKey(tableId))
                        {
                            differences.put(tableId, new ArrayList<>());
                        }

                        differences.get(tableId).add(Math.abs(oldScore - newResult.getSecond()));
                    }
                }
            }
        }

        return differences;
    }
}
