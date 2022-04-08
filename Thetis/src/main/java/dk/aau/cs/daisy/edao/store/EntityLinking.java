package dk.aau.cs.daisy.edao.store;

import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.IdDictionary;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Mapping from Wikipedia link to KG entity URI
 * Warning: This can be slow!
 * A trie is maybe better, where leafs contain IDs and no duplicate bidirectional mapping.
 */
public class EntityLinking implements Linker<String, String>
{
    private IdDictionary<String> dict;
    private Map<Id, Id> mapping;    // Wikipedia link to entity URI

    public EntityLinking()
    {
        this.dict = new IdDictionary<>(false);
        this.mapping = new HashMap<>();
    }

    public EntityLinking(IdDictionary<String> dict)
    {
        this();
        this.dict = dict;
    }

    public IdDictionary<String> getDictionary()
    {
        return this.dict;
    }

    /**
     * Mapping from Wikipedia link to KG entity URI
     * @param wikipedia link
     * @return Entity URI or null if absent
     */
    @Override
    public String mapTo(String wikipedia)
    {
        Id wikiId = this.dict.get(wikipedia);

        if (wikiId == null)
            return null;

        Id uriId = this.mapping.get(wikiId);

        if (uriId == null)
            return null;

        return getKey(uriId);
    }

    /**
     * Mapping from KG entity URI to Wikipedia link
     * @param uri of KG entity
     * @return Wikipedia link or null if absent
     */
    @Override
    public String mapFrom(String uri)
    {
        Id uriId = this.dict.get(uri), wikiId = null;

        if (uriId == null)
            return null;

        for (Map.Entry<Id, Id> entry : this.mapping.entrySet())
        {
            if (entry.getValue().equals(uriId))
            {
                wikiId = entry.getKey();
                break;
            }
        }

        if (wikiId == null)
            return null;

        return getKey(wikiId);
    }

    /**
     * Adds mapping
     * @param wikipedia link
     * @param uri of KG entity
     */
    @Override
    public void addMapping(String wikipedia, String uri)
    {
        Id wikiId = this.dict.get(wikipedia), uriId = this.dict.get(uri);

        if (wikiId == null)
            this.dict.put(wikipedia, (wikiId = Id.alloc()));

        if (uriId == null)
            this.dict.put(uri, (uriId = Id.alloc()));

        this.mapping.putIfAbsent(wikiId, uriId);
    }

    private String getKey(Id id)
    {
        Iterator<String> iter = this.dict.keys().asIterator();

        while (iter.hasNext())
        {
            String key = iter.next();

            if (this.dict.get(key).equals(id))
                return key;
        }

        return null;
    }
}
