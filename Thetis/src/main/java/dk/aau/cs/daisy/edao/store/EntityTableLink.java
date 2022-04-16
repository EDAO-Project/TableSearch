package dk.aau.cs.daisy.edao.store;

import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.Pair;

import java.util.*;

/**
 * Inverted indexing of entities' link to table names
 */
public class EntityTableLink implements Index<Id, List<String>>
{
    private Map<Id, Map<String, List<Pair<Integer, Integer>>>> idx;   // Indexing from entity to table file names of locations where the entity is found

    public EntityTableLink()
    {
        this.idx = new HashMap<>();
    }

    @Override
    public void insert(Id key, List<String> fileNames)
    {
        for (String fileName : fileNames)
        {
            if (this.idx.containsKey(key))
            {
                if (!this.idx.get(key).containsKey(fileName))
                    this.idx.get(key).put(fileName, new ArrayList<>());
            }

            else
            {
                Map<String, List<Pair<Integer, Integer>>> locations = new HashMap<>();
                locations.put(fileName, new ArrayList<>());
                this.idx.put(key, locations);
            }
        }
    }

    @Override
    public boolean remove(Id key)
    {
        return this.idx.remove(key) != null;
    }

    /**
     * Finds list of table file names for given entity ID
     * Order of table file names is not guaranteed
     * @param key Entity ID
     * @return List of table file names
     */
    @Override
    public List<String> find(Id key)
    {
        Map<String, List<Pair<Integer, Integer>>> tablesLocations = this.idx.get(key);

        if (tablesLocations == null)
            return null;

        return List.copyOf(tablesLocations.keySet());
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

    /**
     * Adds location of entity in given table file name
     * @param key Entity ID
     * @param fileName Name of table file
     * @param locations Locations in file name of given entity
     */
    public void addLocation(Id key, String fileName, List<Pair<Integer, Integer>> locations)
    {
        if (this.idx.containsKey(key))
        {
            if (this.idx.get(key).containsKey(fileName))
                locations.forEach(l -> this.idx.get(key).get(fileName).add(l));

            else
            {
                this.idx.get(key).put(fileName, new ArrayList<>(locations.size()));
                locations.forEach(l -> this.idx.get(key).get(fileName).add(l));
            }
        }

        else
        {
            Map<String, List<Pair<Integer, Integer>>> fileNamesLocations = new HashMap<>();
            List<Pair<Integer, Integer>> locationsCopy = new ArrayList<>(locations.size());
            locationsCopy.addAll(locations);
            fileNamesLocations.put(fileName, locationsCopy);
            this.idx.put(key, fileNamesLocations);
        }
    }

    /**
     * Gets all locations of entity in table file
     * @param key Entity ID
     * @param fileName Name of table file
     * @return List of locations of the given entity in given table file
     */
    public List<Pair<Integer, Integer>> getLocations(Id key, String fileName)
    {
        if (!this.idx.containsKey(key))
            return null;

        return this.idx.get(key).get(fileName);
    }

    /**
     * Mapping from table file name to set of entities to which the table is linked
     * This method can be slow
     * This is a substitution for the tableIDTOEntities map
     * @param fileName Table file name
     * @return Set of entities the table links to
     */
    public Set<Id> tableToEntities(String fileName)
    {
        Set<Id> entities = new HashSet<>();

        for (Map.Entry<Id, Map<String, List<Pair<Integer, Integer>>>> entry : this.idx.entrySet())
        {
            if (this.idx.get(entry.getKey()).containsKey(fileName))
                entities.add(entry.getKey());
        }

        return entities;
    }
}
