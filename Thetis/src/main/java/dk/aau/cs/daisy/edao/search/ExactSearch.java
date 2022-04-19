package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.table.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExactSearch extends AbstractSearch
{
    private long elapsed = -1;

    public ExactSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink)
    {
        super(linker, entityTable, entityTableLink);
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
                Id entityID = getLinker().getDictionary().get(flattenedQuery.get(i));
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
        Id firstEntity = getLinker().getDictionary().get(row.get(0));
        List<String> sharedTableFiles = getEntityTableLink().find(firstEntity);

        for (int i = 1; i < queryEntityCount; i++)
        {
            Id entityID = getLinker().getDictionary().get(row.get(i));
            sharedTableFiles.retainAll(getEntityTableLink().find(entityID));
        }

        return sharedTableFiles;
    }
}
