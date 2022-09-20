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
        frequencies.sort((t1, t2) -> t2.getValue() - t1.getValue());

        Iterator<Map.Entry<String, Integer>> iter = frequencies.iterator();
        Set<String> popularTypes = new HashSet<>();
        int sum = 0, types = occurrences.values().stream().reduce(0, Integer::sum);

        while ((double) sum / types < percentile && iter.hasNext())
        {
            Map.Entry<String, Integer> entry = iter.next();
            popularTypes.add(entry.getKey());
            sum += entry.getValue();
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
