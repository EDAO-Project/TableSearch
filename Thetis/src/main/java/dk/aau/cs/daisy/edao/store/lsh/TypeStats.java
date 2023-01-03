package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.graph.Type;
import dk.aau.cs.daisy.edao.structures.table.Table;

import java.util.*;
import java.util.stream.Collectors;

public final class TypeStats
{
    private EntityTable entityTable;

    public TypeStats(EntityTable entityTable)
    {
        this.entityTable = entityTable;
    }

    /**
     * The types included in the percentile of type frequency among entities.
     * @param percentile Between 0 and 1.0. E.g., 90th percentile is 0.9.
     * @return The most frequent types for entities within a given percentile.
     */
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

    /**
     * Most popular types according to type percentages in tables.
     * @param percentage Determines the percentage within which the type must be to be included.
     * @param tables Corpus of tables to compute percentages from.
     * @param linker To perform lookup from entity string to its numeric ID
     * @return Set of types included in the top-K most frequent types found in tables, where K is a given percentage.
     */
    public Set<String> popularByTable(double percentage, Set<Table<String>> tables, EntityLinking linker)
    {
        Set<String> types = countOccurrences().keySet();
        Map<String, Integer> typeCountInTables = new HashMap<>();

        for (String type : types)
        {
            for (Table<String> table : tables)
            {
                if (hasType(table, type, linker))
                {
                    typeCountInTables.put(type,
                            typeCountInTables.containsKey(type) ? typeCountInTables.get(type) + 1 : 1);
                }
            }
        }

        Map<String, Double> typePercentages = percentages(typeCountInTables);
        List<Map.Entry<String, Double>> sorted = new ArrayList<>(typePercentages.entrySet());
        sorted.sort(Comparator.comparingDouble(Map.Entry::getValue));

        List<String> sortedTypes = sorted.stream()
                .filter(pair -> pair.getValue() >= percentage)
                .map(Map.Entry::getKey)
                .toList();
        return new HashSet<>(sortedTypes);
    }

    private boolean hasType(Table<String> table, String type, EntityLinking linker)
    {
        int rows = table.rowCount();

        for (int row = 0; row < rows; row++)
        {
            int columns = table.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                String entity = linker.mapTo(table.getRow(row).get(column));

                if (entity == null)
                {
                    continue;
                }

                Id id = linker.kgUriLookup(entity);

                if (id != null)
                {
                    Set<String> entityTypes = this.entityTable.find(id)
                            .getTypes()
                            .stream()
                            .map(Type::getType)
                            .collect(Collectors.toSet());


                    if (entityTypes.contains(type))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static <E> Map<E, Double> percentages(Map<E, Integer> map)
    {
        int count = map.size();
        Map<E, Double> percentages = new HashMap<>(map.size());

        for (Map.Entry<E, Integer> entry : map.entrySet())
        {
            double fraction = (double) entry.getValue() / count;
            percentages.put(entry.getKey(), fraction);
        }

        return percentages;
    }
}
