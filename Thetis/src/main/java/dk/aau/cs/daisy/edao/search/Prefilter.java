package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.store.EmbeddingsIndex;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.store.lsh.TypesLSHIndex;
import dk.aau.cs.daisy.edao.store.lsh.VectorLSHIndex;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;

import java.util.*;

/**
 * Searches corpus using specified LSH index
 * This class is used for pre-filtering the search space
 */
public class Prefilter extends AbstractSearch
{
    private long elapsed = -1;
    private TypesLSHIndex typesLSH;
    private VectorLSHIndex vectorsLSH;
    private static final int SIZE_THRESHOLD = 8;
    private static final int SPLITS_SIZE = 3;
    private static final int MIN_EXISTS_IN = 2;

    private Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, EmbeddingsIndex<String> embeddingsIndex)
    {
        super(linker, entityTable, entityTableLink, embeddingsIndex);
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                     EmbeddingsIndex<String> embeddingsIndex, TypesLSHIndex typesLSHIndex)
    {
        this(linker, entityTable, entityTableLink, embeddingsIndex);
        this.typesLSH = typesLSHIndex;
        this.vectorsLSH = null;
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
            EmbeddingsIndex<String> embeddingsIndex, VectorLSHIndex vectorLSHIndex)
    {
        this(linker, entityTable, entityTableLink, embeddingsIndex);
        this.vectorsLSH = vectorLSHIndex;
        this.typesLSH = null;
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        List<Pair<String, Double>> candidates = new ArrayList<>();
        List<Table<String>> subQueries = List.of(query);
        Map<String, Integer> tableCounter = new HashMap<>();
        boolean isQuerySplit = false;

        if (query.rowCount() >= SIZE_THRESHOLD)
        {
            subQueries = split(query, SPLITS_SIZE);
            isQuerySplit = true;
        }

        for (Table<String> subQuery : subQueries)
        {
            Set<String> subCandidates = searchFromTable(subQuery);
            subCandidates.forEach(t -> tableCounter.put(t, tableCounter.containsKey(t) ? tableCounter.get(t) + 1 : 1));
        }

        for (Map.Entry<String, Integer> entry : tableCounter.entrySet())
        {
            if (isQuerySplit && entry.getValue() >= MIN_EXISTS_IN)
            {
                candidates.add(new Pair<>(entry.getKey(), -1.0));
            }

            else
            {
                candidates.add(new Pair<>(entry.getKey(), -1.0));
            }
        }

        this.elapsed = System.nanoTime() - start;
        return new Result(candidates.size(), candidates);
    }

    private Set<String> searchFromTable(Table<String> query)
    {
        int rows = query.rowCount();
        Set<String> candidates = new HashSet<>();

        for (int row = 0; row < rows; row++)
        {
            int columns = query.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                candidates.addAll(searchLSH(query.getRow(row).get(column)));
            }
        }

        return candidates;
    }

    private static List<Table<String>> split(Table<String> table, int splitSize)
    {
        List<Table<String>> subTables = new ArrayList<>();
        int rows = table.rowCount();

        for (int i = 0; i < rows;)
        {
            Table<String> subTable = new DynamicTable<>();

            for (int j = 0; j < splitSize && i < rows; i++, j++)
            {
                subTable.addRow(table.getRow(i));
            }

            subTables.add(subTable);
        }

        return subTables;
    }

    private Set<String> searchLSH(String entity)
    {
        if (this.typesLSH != null)
        {
            return this.typesLSH.search(entity);
        }

        return this.vectorsLSH.search(entity);
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }
}
