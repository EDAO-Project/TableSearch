package com.thetis.search;

import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class for debugging searching tables that are exact matches to query
 */
public class ExactSearch extends AbstractSearch
{
    private long elapsed = -1;

    public ExactSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, EmbeddingsIndex<String> embeddingsIndex)
    {
        super(linker, entityTable, entityTableLink, embeddingsIndex);
    }

    /**
     * Entry point for exact search
     * @param query Query table
     * @return Result instance of top-K highest ranking tables
     */
    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        Table.Row<String> flattenedQuery = flattenQuery(query);
        int entityCount = flattenedQuery.size();
        List<String> sharedTableFiles = sharedQueryRowFiles(flattenedQuery);
        List<Pair<String, Double>> tableEntityMatches = new ArrayList<>();

        for (String fileName : sharedTableFiles)
        {
            Set<Integer> sharedRows = new HashSet<>();

            for (int i = 0; i < entityCount; i++)
            {
                String uri = getLinker().mapTo(flattenedQuery.get(i));
                Id entityID = getLinker().kgUriLookup(uri);
                List<Pair<Integer, Integer>> locations = getEntityTableLink().getLocations(entityID, fileName);
                Set<Integer> rows = new HashSet<>(locations.size());
                locations.forEach(l -> rows.add(l.getFirst()));

                if (i == 0)
                    sharedRows.addAll(rows);

                sharedRows.retainAll(rows);
            }

            tableEntityMatches.add(new Pair<>(fileName, (double) sharedRows.size()));
        }

        this.elapsed = System.nanoTime() - start;
        return new Result(sharedTableFiles.size(), tableEntityMatches);
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }

    private static Table.Row<String> flattenQuery(Table<String> query)
    {
        int rows = query.rowCount();
        List<String> flattened = new ArrayList<>();

        for (int i = 0; i < rows; i++)
        {
            Table.Row<String> row = query.getRow(i);

            for (int j = 0; j < row.size(); j++)
            {
                flattened.add(row.get(j));
            }
        }

        return new Table.Row<>(flattened);
    }

    private List<String> sharedQueryRowFiles(Table.Row<String> row)
    {
        int queryEntityCount = row.size();
        String uri = getLinker().mapTo(row.get(0));
        Id firstEntity = getLinker().kgUriLookup(uri);
        List<String> sharedTableFiles = getEntityTableLink().find(firstEntity);

        for (int i = 1; i < queryEntityCount; i++)
        {
            uri = getLinker().mapTo(row.get(i));
            Id entityID = getLinker().kgUriLookup(uri);
            sharedTableFiles.retainAll(getEntityTableLink().find(entityID));
        }

        return sharedTableFiles;
    }
}
