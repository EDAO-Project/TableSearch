package com.thetis.loader.progressive.adapter;

import com.thetis.store.hnsw.HNSW;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TopicAdapter implements IndexingAdapter
{
    private final Table<String> query;
    private final HNSW hnsw;
    private static final int HNSW_K = 1000;

    public TopicAdapter(Table<String> query, HNSW hnsw)
    {
        this.query = query;
        this.hnsw = hnsw;
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
        Map<String, Integer> frequencies = new HashMap<>();

        for (int row = 0; row < rows; row++)
        {
            int columns = this.query.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                String entity = this.query.getRow(row).get(column);
                Set<String> tables = this.hnsw.find(entity);
                tables.forEach(table -> {
                    if (!frequencies.containsKey(table))
                    {
                        frequencies.put(table, 0);
                    }

                    frequencies.put(table, frequencies.get(table) + 1);
                });
            }
        }

        return frequencies.entrySet().stream()
                                    .map(entry -> new Pair<>(entry.getKey(), (double) entry.getValue()))
                                    .collect(Collectors.toList());
    }
}
