package com.thetis.loader.progressive.adapter;

import com.thetis.loader.progressive.Indexable;
import com.thetis.store.hnsw.HNSW;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;

import java.util.*;
import java.util.stream.Collectors;

public class TopicAdapter implements IndexingAdapter
{
    private final Table<String> query;
    private final HNSW hnsw;
    private final Map<String, Indexable> indexables;
    private static final int HNSW_K = 1000;

    public TopicAdapter(Table<String> query, HNSW hnsw, Map<String, Indexable> indexables)
    {
        this.query = query;
        this.hnsw = hnsw;
        this.indexables = indexables;
        this.hnsw.setK(HNSW_K);
    }

    /**
     * Computes the priority increment for tables sharing topic with the query table
     * @return Priority increment for related tables
     */
    @Override
    public List<Pair<String, Double>> newPriorities()
    {
        int rows = this.query.rowCount();
        Set<String> tables = new HashSet<>();
        List<Pair<String, Double>> priorityIncrements = new ArrayList<>();

        for (int row = 0; row < rows; row++)
        {
            for (int column = 0; column < this.query.getRow(row).size(); column++)
            {
                String entity = this.query.getRow(row).get(column);
                Set<String> relevantEntityTables = this.hnsw.find(entity);
                tables.addAll(relevantEntityTables);
            }
        }

        Set<Indexable> relevantIndexables = this.indexables.entrySet().stream()
                                            .filter(entry -> tables.contains(entry.getKey()))
                                            .map(Map.Entry::getValue)
                                            .collect(Collectors.toSet());

        relevantIndexables.stream()
                .min(Comparator.comparingInt(indexable -> (int) indexable.getPriority()))
                .ifPresent(minIndexable -> {
                    int newPriority = Math.max(0, (int) minIndexable.getPriority() - 1);
                    relevantIndexables.forEach(indexable -> priorityIncrements.add(new Pair<>(indexable.getId(),
                            indexable.getPriority() - minIndexable.getPriority())));
                });

        return priorityIncrements;
    }
}
