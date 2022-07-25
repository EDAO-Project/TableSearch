package dk.aau.cs.daisy.edao.store;

import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.Pair;

import java.io.*;
import java.util.*;

/**
 * Inverted indexing of entities' link to table names
 * TODO: We can reduce space by storing a separate structure of table names
 *       The index should point by index to the table name in that separate structure along its location within
 *       This way, we remove duplicating table names
 */
public class EntityTableLink implements Index<Id, List<String>>, Externalizable
{
    private Map<Id, Map<String, List<Pair<Integer, Integer>>>> idx;   // Indexing from entity to table file names of locations where the entity is found
    private String dir = null;

    public EntityTableLink()
    {
        this.idx = new HashMap<>();
    }

    public void setDirectory(String dir)
    {
        this.dir = dir;
    }

    public String getDirectory()
    {
        return this.dir;
    }

    /**
     * Insertion of entity mention in table
     * @param key Entity URI ID
     * @param fileNames File name of table. This must exclude the directory.
     */
    @Override
    public void insert(Id key, List<String> fileNames)
    {
        if (!this.idx.containsKey(key))
            this.idx.put(key, new HashMap<>());

        for (String fileName : fileNames)
        {
            if (!this.idx.get(key).containsKey(fileName))
                this.idx.get(key).put(fileName, new ArrayList<>());
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
     * An empty list is returned when ID does not exist
     * @param key Entity ID
     * @return List of table file names
     */
    @Override
    public List<String> find(Id key)
    {
        Map<String, List<Pair<Integer, Integer>>> tablesLocations = this.idx.get(key);

        if (tablesLocations == null)
            return new ArrayList<>();

        return new ArrayList<>(tablesLocations.keySet());
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
            if (!this.idx.get(key).containsKey(fileName))
                this.idx.get(key).put(fileName, new ArrayList<>(locations.size()));

            locations.forEach(l -> this.idx.get(key).get(fileName).add(l));
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

    /**
     * Clears index
     */
    @Override
    public void clear()
    {
        this.idx.clear();
    }

    /**
     * This method should not be called by client
     * This is used to write class object to a stream
     * @param out the stream to write the object to
     * @throws IOException
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        List<Pair<Id, Pair<String, Pair<Integer, Integer>>>> tuples = new ArrayList<>();

        for (Id id : this.idx.keySet())
        {
            for (String fileName : this.idx.get(id).keySet())
            {
                if (this.idx.get(id).get(fileName).isEmpty())
                {
                    tuples.add(new Pair<>(id, new Pair<>(fileName, new Pair<>(-1, -1))));
                    continue;
                }

                for (Pair<Integer, Integer> location : this.idx.get(id).get(fileName))
                {
                    tuples.add(new Pair<>(id, new Pair<>(fileName, new Pair<>(location.getFirst(), location.getSecond()))));
                }
            }
        }

        out.writeObject(tuples);

        if (this.idx == null)
            out.writeObject("null");

        else
            out.writeObject(this.dir);
    }

    /**
     * This method should not be called by client
     * This is used to read class object from a stream
     * @param in the stream to read data from in order to restore the object
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        List<Pair<Id, Pair<String, Pair<Integer, Integer>>>> tuples =
                (List<Pair<Id, Pair<String, Pair<Integer, Integer>>>>) in.readObject();

        for (Pair<Id, Pair<String, Pair<Integer, Integer>>> tuple : tuples)
        {
            if (tuple.getSecond().getSecond().getFirst() == -1 && tuple.getSecond().getSecond().getSecond() == -1)
                insert(tuple.getFirst(), List.of(tuple.getSecond().getFirst()));

            else
                addLocation(tuple.getFirst(), tuple.getSecond().getFirst(), List.of(tuple.getSecond().getSecond()));
        }

        String dir = (String) in.readObject();

        if (dir.equals("null"))
            this.dir = null;

        else
            this.dir = dir;
    }
}
