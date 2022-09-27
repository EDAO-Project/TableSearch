package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.graph.Type;

import java.util.*;

public final class TypeStats
{
    private EntityTable entityTable;

    public TypeStats(EntityTable entityTable)
    {
        this.entityTable = entityTable;
    }

    public Set<String> popularByPercentile(double percentile)
    {
        if (percentile < 0 || percentile > 1)
        {
            throw new IllegalArgumentException("Percentile must be between 0.0 and 1.0");
        }

        Map<String, Integer> occurrences = countOccurrences();
        List<Map.Entry<String, Integer>> frequencies = new ArrayList<>(occurrences.entrySet());
        frequencies.sort(Comparator.comparingInt(Map.Entry::getValue));

        int percentilePosition = (int) (percentile * frequencies.size());
        Set<String> popularTypes = new HashSet<>();

        for (int i = percentilePosition; i < frequencies.size(); i++)
        {
            popularTypes.add(frequencies.get(i).getKey());
        }

        return popularTypes;
    }

    private Map<String, Integer> countOccurrences()
    {
        Iterator<Id> ids = this.entityTable.allIds();
        Map<String, Integer> count = new HashMap<>();

        while (ids.hasNext())
        {
            Id id = ids.next();
            List<Type> entityTypes = this.entityTable.find(id).getTypes();

            for (Type t : entityTypes)
            {
                if (count.containsKey(t.getType()))
                {
                    count.put(t.getType(), count.get(t.getType()) + 1);
                }

                else
                {
                    count.put(t.getType(), 1);
                }
            }
        }

        return count;
    }
}
