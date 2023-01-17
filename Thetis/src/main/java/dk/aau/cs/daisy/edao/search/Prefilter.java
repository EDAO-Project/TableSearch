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
        Set<String> candidates = new HashSet<>();

        if (query.rowCount() == 0)
        {
            return candidates;
        }

        int rows = query.rowCount(), columns = query.getRow(0).size();

        for (int column = 0; column < columns; column++)
        {
            Set<String> entities = new HashSet<>(rows);

            for (int row = 0; row < rows; row++)
            {
                if (column < query.getRow(row).size())
                {
                    entities.add(query.getRow(row).get(column));
                }
            }

            candidates.addAll(searchLSH(entities));
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

    private Set<String> searchLSH(Set<String> entities)
    {
        String[] entityArr = new String[entities.size()];
        int i = 0;

        for (String entity : entities)
        {
            entityArr[i++] = entity;
        }

        if (this.typesLSH != null)
        {
            return entityArr.length == 1 ?
                    this.typesLSH.search(entityArr[0]) :
                    this.typesLSH.agggregatedSearch(entityArr);
        }

        return entityArr.length == 1 ?
                this.vectorsLSH.search(entityArr[0]) :
                this.vectorsLSH.agggregatedSearch(entityArr);
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }
}
