package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.store.lsh.TypesLSHIndex;
import dk.aau.cs.daisy.edao.store.lsh.VectorLSHIndex;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Searches corpus using specified LSH index
 * This class is used for pre-filtering the search space
 */
public class Prefilter extends AbstractSearch
{
    private long elapsed = -1;
    private TypesLSHIndex typesLSH;
    private VectorLSHIndex vectorsLSH;

    private Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink)
    {
        super(linker, entityTable, entityTableLink);
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, TypesLSHIndex typesLSHIndex)
    {
        this(linker, entityTable, entityTableLink);
        this.typesLSH = typesLSHIndex;
        this.vectorsLSH = null;
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, VectorLSHIndex vectorLSHIndex)
    {
        this(linker, entityTable, entityTableLink);
        this.vectorsLSH = vectorLSHIndex;
        this.typesLSH = null;
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        int rows = query.rowCount();
        List<Pair<String, Double>> candidates = new ArrayList<>();

        for (int row = 0; row < rows; row++)
        {
            int columns = query.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                Set<String> entityCandidates = searchLSH(query.getRow(row).get(column));
                entityCandidates.forEach(t -> candidates.add(new Pair<>(t, -1.0)));
            }
        }

        this.elapsed = System.nanoTime() - start;
        return new Result(candidates.size(), candidates);
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
