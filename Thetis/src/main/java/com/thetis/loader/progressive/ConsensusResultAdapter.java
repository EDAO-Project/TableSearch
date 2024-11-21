package com.thetis.loader.progressive;

import com.thetis.search.Result;
import com.thetis.structures.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class ConsensusResultAdapter extends ResultAdapter implements IndexingAdapter
{
    private final Set<String> tableIds = new HashSet<>();

    public ConsensusResultAdapter(Set<Pair<Result, Result>> results)
    {
        super(results);
    }

    @Override
    public List<Pair<String, Double>> newPriorities(Map<String, Double> currentPriorities)
    {
        List<Pair<String, Double>> priorities = new ArrayList<>();
        Map<String, Double> frequencies = frequencyFractions();
        Map<String, List<Double>> relevanceDifferences = relevanceDifferences();
        Map<String, Double> maxRelevanceDifferences = new HashMap<>();
        Map<String, Double> improvements = improvements();
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

        for (String tableId : this.tableIds)
        {
            if (!currentPriorities.containsKey(tableId))
            {
                continue;
            }

            double currentPriority = currentPriorities.get(tableId);
            double maxDiff = maxRelevanceDifferences.get(tableId);

            if (maxDiff < theta)
            {
                double freq = frequencies.get(tableId);
                priorities.add(new Pair<>(tableId, currentPriority - Math.abs(currentPriority) * freq));
            }

            else if (improvements.get(tableId) > 0)
            {
                priorities.add(new Pair<>(tableId, currentPriority + Math.abs(currentPriority) * improvements.get(tableId)));
            }

            else
            {
                priorities.add(new Pair<>(tableId, currentPriority));
            }
        }

        return priorities;
    }

    private Map<String, Double> frequencyFractions()
    {
        Map<String, Integer> counts = new HashMap<>();

        for (Pair<Result, Result> results : getResults())
        {
            Result newResults = results.getSecond();
            Set<String> tableIds = newResults.getResultSet()
                                                .stream()
                                                .map(Pair::getFirst)
                                                .collect(Collectors.toSet());

            for (String tableId : tableIds)
            {
                if (!counts.containsKey(tableId))
                {
                    counts.put(tableId, 0);
                }

                counts.put(tableId, counts.get(tableId) + 1);
            }
        }

        int resultSets = getResults().size();
        Map<String, Double> fractions = new HashMap<>();
        counts.forEach((tableId, count) -> fractions.put(tableId, (double) count / resultSets));

        return fractions;
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

    private Map<String, Double> improvements()
    {
        Map<String, Double> improvements = new HashMap<>();
        Set<Pair<Result, Result>> allResults = getResults();

        for (Pair<Result, Result> results : allResults)
        {
            Map<String, Double> newResultsMap = new HashMap<>();

            for (Pair<String, Double> newResults : results.getSecond().getResultSet())
            {
                newResultsMap.put(newResults.getFirst(), newResults.getSecond());
            }

            for (Pair<String, Double> oldResult : results.getFirst().getResultSet())
            {
                String tableId = oldResult.getFirst();

                if (newResultsMap.containsKey(tableId))
                {
                    this.tableIds.add(tableId); // This is a hack to store all re-retrieved tables to optimize runtime a little bit

                    if (!improvements.containsKey(tableId))
                    {
                        improvements.put(tableId, 0.0);
                    }

                    double improvement = newResultsMap.get(tableId) - oldResult.getSecond();
                    improvements.replace(tableId, improvements.get(tableId) + improvement);
                }
            }
        }

        int resultSets = allResults.size();
        improvements.replaceAll((tableId, sum) -> sum / resultSets);

        return improvements;
    }
}
