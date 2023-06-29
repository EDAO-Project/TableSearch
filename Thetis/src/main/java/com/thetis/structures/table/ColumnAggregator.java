package com.thetis.structures.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * Aggregator of table columns
 * @param <C> Type of table cells
 */
public class ColumnAggregator<C> implements Aggregator<C>
{
    private Table<C> table;

    public ColumnAggregator(Table<C> table)
    {
        this.table = table;
    }

    /**
     * Aggregator method
     * @param mapper Mapping function from cell to some value
     * @param aggregator Aggregator function that aggregated all content of a column
     * @return List of aggregated columns
     * @param <E>
     */
    public <E> List<E> aggregate(Function<C, E> mapper, Function<Collection<E>, E> aggregator)
    {
        if (this.table.rowCount() == 0)
        {
            return new ArrayList<>();
        }

        int rows = this.table.rowCount(), columns = this.table.getRow(0).size();
        List<E> aggregatedColumns = new ArrayList<>(columns);

        for (int column = 0; column < columns; column++)
        {
            List<E> columnContent = new ArrayList<>(rows);

            for (int row = 0; row < rows; row++)
            {
                if (column < table.getRow(row).size())
                {
                    E mapped = mapper.apply(this.table.getRow(row).get(column));

                    if (mapped != null)
                    {
                        columnContent.add(mapped);
                    }
                }
            }

            E aggregated = aggregator.apply(columnContent);
            aggregatedColumns.add(aggregated);
        }

        return aggregatedColumns;
    }
}
