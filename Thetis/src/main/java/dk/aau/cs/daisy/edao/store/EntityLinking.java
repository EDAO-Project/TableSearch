package dk.aau.cs.daisy.edao.store;

import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.IdDictionary;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapping from Wikipedia link to KG entity URI
 * A trie is maybe better, where leafs contain IDs and no duplicate bidirectional mapping.
 */
public class EntityLinking implements Linker<String, String>, Serializable
{
    private IdDictionary<String> dict;
    private Map<Id, Id> wikiLinkToUri;    // Wikipedia link to entity URI
    private Map<Id, Id> uriToWikiLink;    // entity URI to Wikipedia link

    public EntityLinking()
    {
        this.dict = new IdDictionary<>(false);
        this.wikiLinkToUri = new HashMap<>();
        this.uriToWikiLink = new HashMap<>();
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

        Id uriId = this.wikiLinkToUri.get(wikiId);

        if (uriId == null)
            return null;

        return this.dict.get(uriId);
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

        wikiId = this.uriToWikiLink.get(uriId);

        if (wikiId == null)
            return null;

        return this.dict.get(wikiId);
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

        this.wikiLinkToUri.putIfAbsent(wikiId, uriId);
        this.uriToWikiLink.putIfAbsent(uriId, wikiId);
    }

    /**
     * Clears mappings and dictionary
     */
    @Override
    public void clear()
    {
        this.wikiLinkToUri.clear();
        this.uriToWikiLink.clear();
        this.dict.clear();
    }
}
