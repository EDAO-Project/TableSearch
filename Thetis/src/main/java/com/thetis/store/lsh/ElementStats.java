package com.thetis.store.lsh;

import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import com.thetis.structures.table.Table;

import java.util.*;
import java.util.stream.Collectors;

public final class ElementStats
{
    private EntityTable entityTable;
    private SetLSHIndex.EntitySet setType;

    public ElementStats(EntityTable entityTable, SetLSHIndex.EntitySet setType)
    {
        this.entityTable = entityTable;
        this.setType = setType;
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
            Entity entity = this.entityTable.find(id);
            List<?> entityElements = this.setType == SetLSHIndex.EntitySet.TYPES ? entity.getTypes() : entity.getPredicates();

            for (var v : entityElements)
            {
                String element = this.setType == SetLSHIndex.EntitySet.TYPES ? ((Type) v).getType() : (String) v;

                if (count.containsKey(element))
                {
                    count.put(element, count.get(element) + 1);
                }

                else
                {
                    count.put(element, 1);
                }
            }
        }

        return count;
    }

    /**
     * Most popular elements according to element percentages in tables.
     * @param percentage Determines the percentage within which the element must be to be included.
     * @param tables Corpus of tables to compute percentages from.
     * @param linker To perform lookup from entity string to its numeric ID
     * @return Set of elements included in the top-K most frequent elements found in tables, where K is a given percentage.
     */
    public Set<String> popularByTable(double percentage, Set<Table<String>> tables, EntityLinking linker)
    {
        Set<String> elements = countOccurrences().keySet();
        Map<String, Integer> elementCountInTables = new HashMap<>();

        for (String element : elements)
        {
            for (Table<String> table : tables)
            {
                if (hasElement(table, element, linker))
                {
                    elementCountInTables.put(element,
                            elementCountInTables.containsKey(element) ? elementCountInTables.get(element) + 1 : 1);
                }
            }
        }

        Map<String, Double> elementPercentages = percentages(elementCountInTables, tables.size());
        List<Map.Entry<String, Double>> sorted = new ArrayList<>(elementPercentages.entrySet());
        sorted.sort(Comparator.comparingDouble(Map.Entry::getValue));

        List<String> sortedTypes = sorted.stream()
                .filter(pair -> pair.getValue() >= percentage)
                .map(Map.Entry::getKey)
                .toList();
        return new HashSet<>(sortedTypes);
    }

    private boolean hasElement(Table<String> table, String element, EntityLinking linker)
    {
        int rows = table.rowCount();

        for (int row = 0; row < rows; row++)
        {
            int columns = table.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                String entity = table.getRow(row).get(column);

                if (entity == null)
                {
                    continue;
                }

                Id id = linker.kgUriLookup(entity);

                if (id != null)
                {
                    Set<String> entityElements = this.entityTable.find(id)
                            .getTypes()
                            .stream()
                            .map(Type::getType)
                            .collect(Collectors.toSet());

                    if (this.setType == SetLSHIndex.EntitySet.PREDICATES)
                    {
                        entityElements = new HashSet<>(this.entityTable.find(id).getPredicates());
                    }

                    if (entityElements.contains(element))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static <E> Map<E, Double> percentages(Map<E, Integer> map, int corpusSize)
    {
        Map<E, Double> percentages = new HashMap<>(map.size());

        for (Map.Entry<E, Integer> entry : map.entrySet())
        {
            double fraction = (double) entry.getValue() / corpusSize;
            percentages.put(entry.getKey(), fraction);
        }

        return percentages;
    }
}
