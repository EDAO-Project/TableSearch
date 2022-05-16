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
    private IdDictionary<String> uriDict, wikiDict;
    private Map<Id, Id> wikiLinkToUri;    // Wikipedia link to entity URI
    private Map<Id, Id> uriToWikiLink;    // entity URI to Wikipedia link
    String wikiPrefix, uriPrefix;

    public EntityLinking(String wikiPrefix, String uriPrefix)
    {
        this.uriDict = new IdDictionary<>(false);
        this.wikiDict = new IdDictionary<>(false);
        this.wikiLinkToUri = new HashMap<>();
        this.uriToWikiLink = new HashMap<>();
        this.wikiPrefix = wikiPrefix;
        this.uriPrefix = uriPrefix;
    }

    public EntityLinking(IdDictionary<String> uriDict, IdDictionary wikiDict, String wikiPrefix, String uriPrefix)
    {
        this(wikiPrefix, uriPrefix);
        this.uriDict = uriDict;
        this.wikiDict = wikiDict;
    }

    public IdDictionary<String> getUriDictionary()
    {
        return this.uriDict;
    }

    public IdDictionary<String> getWikiDictionary()
    {
        return this.wikiDict;
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

        Id wikiId = this.wikiDict.get(wikipedia.substring(this.wikiPrefix.length()));

        if (wikiId == null)
            return null;

        Id uriId = this.wikiLinkToUri.get(wikiId);

        if (uriId == null)
            return null;

        return this.uriPrefix + this.uriDict.get(uriId);
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

        Id uriId = this.uriDict.get(uri.substring(this.uriPrefix.length()));

        if (uriId == null)
            return null;

        Id wikiId = this.uriToWikiLink.get(uriId);

        if (wikiId == null)
            return null;

        return this.wikiPrefix + this.wikiDict.get(wikiId);
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
        Id wikiId = this.wikiDict.get(wikiNoPrefix), uriId = this.uriDict.get(uriNoPrefix);

        if (wikiId == null)
            this.wikiDict.put(wikiNoPrefix, (wikiId = Id.alloc()));

        if (uriId == null)
            this.uriDict.put(uriNoPrefix, (uriId = Id.alloc()));

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
        this.uriDict.clear();
        this.wikiDict.clear();
    }
}
