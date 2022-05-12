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
    String wikiPrefix, uriPrefix;

    public EntityLinking(String wikiPrefix, String uriPrefix)
    {
        this.dict = new IdDictionary<>(false);
        this.wikiLinkToUri = new HashMap<>();
        this.uriToWikiLink = new HashMap<>();
        this.wikiPrefix = wikiPrefix;
        this.uriPrefix = uriPrefix;
    }

    public EntityLinking(IdDictionary<String> dict, String wikiPrefix, String uriPrefix)
    {
        this(wikiPrefix, uriPrefix);
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
        if (!wikipedia.startsWith(this.wikiPrefix))
            throw new IllegalArgumentException("Wikipedia link does not start with specified prefix");

        Id wikiId = this.dict.get(wikipedia.substring(this.wikiPrefix.length()));

        if (wikiId == null)
            return null;

        Id uriId = this.wikiLinkToUri.get(wikiId);

        if (uriId == null)
            return null;

        return this.uriPrefix + this.dict.get(uriId);
    }

    /**
     * Mapping from KG entity URI to Wikipedia link
     * @param uri of KG entity
     * @return Wikipedia link or null if absent
     */
    @Override
    public String mapFrom(String uri)
    {
        if (!uri.startsWith(this.uriPrefix))
            throw new IllegalArgumentException("Entity URI does not start with specified prefix");

        Id uriId = this.dict.get(uri.substring(this.uriPrefix.length()));

        if (uriId == null)
            return null;

        Id wikiId = this.uriToWikiLink.get(uriId);

        if (wikiId == null)
            return null;

        return this.wikiPrefix + this.dict.get(wikiId);
    }

    /**
     * Adds mapping
     * @param wikipedia link
     * @param uri of KG entity
     */
    @Override
    public void addMapping(String wikipedia, String uri)
    {
        if (!wikipedia.startsWith(this.wikiPrefix) || !uri.startsWith(this.uriPrefix))
            throw new IllegalArgumentException("Wikipedia link and/or entity URI do not start with given prefix");

        String wikiNoPrefix = wikipedia.substring(this.wikiPrefix.length()),
                uriNoPrefix = uri.substring(this.uriPrefix.length());
        Id wikiId = this.dict.get(wikiNoPrefix), uriId = this.dict.get(uriNoPrefix);

        if (wikiId == null)
            this.dict.put(wikiNoPrefix, (wikiId = Id.alloc()));

        if (uriId == null)
            this.dict.put(uriNoPrefix, (uriId = Id.alloc()));

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
