package com.thetis.store;

import com.thetis.structures.Id;
import com.thetis.structures.IdDictionary;

import java.io.Serializable;
import java.util.*;

/**
 * Mapping from an entity of one type to another entity of some type
 * Entity types are specified by T1 and T2
 * A trie is maybe better, where leafs contain IDs and no duplicate bidirectional mapping.
 */
public class EntityLinking implements Linker<String, String>, Serializable
{
    private IdDictionary<String> t1Ids, t2Ids;
    private final Map<Id, Id> inputToKGEntity;    // Input entity to KG entity mapping
    private final Map<Id, Id> kgEntityToInput;    // KG entity to input entity
    String inputPrefix, kgEntityPrefix;

    public EntityLinking(String inputPrefix, String kgEntityPrefix)
    {
        this.t1Ids = new IdDictionary<>(false);
        this.t2Ids = new IdDictionary<>(false);
        this.inputToKGEntity = new HashMap<>();
        this.kgEntityToInput = new HashMap<>();
        this.inputPrefix = inputPrefix;
        this.kgEntityPrefix = kgEntityPrefix;
    }

    public EntityLinking(IdDictionary<String> kgEntityDict, IdDictionary<String> inputDict, String inputPrefix, String kgEntityPrefix)
    {
        this(inputPrefix, kgEntityPrefix);
        this.t1Ids = kgEntityDict;
        this.t2Ids = inputDict;
    }

    public Id kgUriLookup(String kgEntity)
    {
        return this.t1Ids.get(kgEntity.substring(this.kgEntityPrefix.length()));
    }

    public String kgUriLookup(Id id)
    {
        return this.kgEntityPrefix + this.t1Ids.get(id);
    }

    public Id inputUriLookup(String inputEntity)
    {
        return this.t2Ids.get(inputEntity.substring(this.inputPrefix.length()));
    }

    public String inputUriLookup(Id id)
    {
        return this.inputPrefix + this.t2Ids.get(id);
    }

    public Iterator<Id> kgUriIds()
    {
        return this.t1Ids.elements().asIterator();
    }

    public Iterator<Id> inputUriIds()
    {
        return this.t2Ids.elements().asIterator();
    }

    /**
     * Mapping from input entity URI to KG entity URI
     * @param inputEntity URI
     * @return Entity URI or null if absent
     */
    @Override
    public String mapTo(String inputEntity)
    {
        if (!inputEntity.startsWith(this.inputPrefix))
            throw new IllegalArgumentException("Input entity URI '" + inputEntity + "' does not start with specified prefix");

        Id inputId = this.t2Ids.get(inputEntity.substring(this.inputPrefix.length()));

        if (inputId == null)
            return null;

        Id kgId = this.inputToKGEntity.get(inputId);

        if (kgId == null)
            return null;

        return this.kgEntityPrefix + this.t1Ids.get(kgId);
    }

    /**
     * Mapping from KG entity URI to input entity URI
     * @param kgUri of KG entity
     * @return input entity URI or null if absent
     */
    @Override
    public String mapFrom(String kgUri)
    {
        if (!kgUri.startsWith(this.kgEntityPrefix))
            throw new IllegalArgumentException("KG entity URI does not start with specified prefix");

        Id uriId = this.t1Ids.get(kgUri.substring(this.kgEntityPrefix.length()));

        if (uriId == null)
            return null;

        Id inputId = this.kgEntityToInput.get(uriId);

        if (inputId == null)
            return null;

        return this.inputPrefix + this.t2Ids.get(inputId);
    }

    /**
     * Adds mapping
     * @param inputEntity URI
     * @param kgEntity of KG entity
     */
    @Override
    public void addMapping(String inputEntity, String kgEntity)
    {
        if (!inputEntity.startsWith(this.inputPrefix) || !kgEntity.startsWith(this.kgEntityPrefix))
            throw new IllegalArgumentException("Input entity URI and/or KG entity URI do not start with given prefix");

        String inputNoPrefix = inputEntity.substring(this.inputPrefix.length()),
                kgUriNoPrefix = kgEntity.substring(this.kgEntityPrefix.length());
        Id inputId = this.t2Ids.get(inputNoPrefix), uriId = this.t1Ids.get(kgUriNoPrefix);

        if (inputId == null)
            this.t2Ids.put(inputNoPrefix, (inputId = Id.alloc()));

        if (uriId == null)
            this.t1Ids.put(kgUriNoPrefix, (uriId = Id.alloc()));

        this.inputToKGEntity.putIfAbsent(inputId, uriId);
        this.kgEntityToInput.putIfAbsent(uriId, inputId);
    }

    /**
     * Clears mappings and dictionary
     */
    @Override
    public void clear()
    {
        this.inputToKGEntity.clear();
        this.kgEntityToInput.clear();
        this.t1Ids.clear();
        this.t2Ids.clear();
    }

    public String getInputPrefix()
    {
        return this.inputPrefix;
    }

    public String getKgEntityPrefix()
    {
        return this.kgEntityPrefix;
    }
}
