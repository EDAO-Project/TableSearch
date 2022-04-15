package dk.aau.cs.daisy.edao.store;

import dk.aau.cs.daisy.edao.structures.graph.Entity;
import dk.aau.cs.daisy.edao.structures.Id;

import java.util.HashMap;
import java.util.Map;

/**
 * Indexing of entities containing types
 */
public class EntityTable implements Index<Id, Entity>
{
    private Map<Id, Entity> idx = new HashMap<>();

    @Override
    public void insert(Id key, Entity value)
    {
        this.idx.put(key, value);
    }

    @Override
    public boolean remove(Id key)
    {
        return this.idx.remove(key) != null;
    }

    @Override
    public Entity find(Id key)
    {
        return this.idx.get(key);
    }

    @Override
    public boolean contains(Id key)
    {
        return this.idx.containsKey(key);
    }

    @Override
    public int size()
    {
        return this.idx.size();
    }
}
