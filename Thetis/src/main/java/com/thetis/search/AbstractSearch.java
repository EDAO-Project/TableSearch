package com.thetis.search;

import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.table.Table;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class AbstractSearch implements TableSearch
{
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;
    private EmbeddingsIndex<Id> embeddingsIndex;

    protected AbstractSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                             EmbeddingsIndex<Id> embeddingIdx)
    {
        this.linker = linker;
        this.entityTable = entityTable;
        this.entityTableLink = entityTableLink;
        this.embeddingsIndex = embeddingIdx;
    }

    protected Set<String> distinctTables()
    {
        Set<String> tables = new HashSet<>();
        Iterator<Id> entityIter = getLinker().kgUriIds();

        while (entityIter.hasNext())
        {
            tables.addAll(getEntityTableLink().find(entityIter.next()));
        }

        return tables;
    }

    @Override
    public Result search(Table<String> query)
    {
        return abstractSearch(query);
    }

    @Override
    public long elapsedNanoSeconds()
    {
        return abstractElapsedNanoSeconds();
    }

    public EntityLinking getLinker()
    {
        return this.linker;
    }

    public EntityTable getEntityTable()
    {
        return this.entityTable;
    }

    public EntityTableLink getEntityTableLink()
    {
        return this.entityTableLink;
    }

    public EmbeddingsIndex<Id> getEmbeddingsIndex()
    {
        return this.embeddingsIndex;
    }

    protected abstract Result abstractSearch(Table<String> query);
    protected abstract long abstractElapsedNanoSeconds();
}
