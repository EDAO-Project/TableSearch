package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.store.lsh.TypesLSHIndex;
import dk.aau.cs.daisy.edao.store.lsh.VectorLSHIndex;
import dk.aau.cs.daisy.edao.structures.table.Table;

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

    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }
}
