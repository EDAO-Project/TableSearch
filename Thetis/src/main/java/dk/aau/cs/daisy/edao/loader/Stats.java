package dk.aau.cs.daisy.edao.loader;

import java.util.List;
import java.util.Set;

public class Stats
{
    private int numRows = -1, numCols = -1, numsCells = -1, numEntities = -1, numMappedCells = -1;
    private long cellToEntityMatches = -1;
    private List<Integer> entitiesPerRow = null, entitiesPerColumn = null, cellToEntityMatchesPerCol = null;
    List<Boolean> numericTableColumns = null;
    private Set<String> entities = null;

    public static class StatBuilder
    {
        private int numRows = -1, numCols = -1, numsCells = -1, numEntities = -1, numMappedCells = -1;
        private long cellToEntityMatches = -1;
        private List<Integer> entitiesPerRow = null, entitiesPerColumn = null, cellToEntityMatchesPerCol = null;
        List<Boolean> numericTableColumns = null;
        private Set<String> entities;

        public StatBuilder rows(int count)
        {
            this.numRows = count;
            return this;
        }

        public StatBuilder columns(int count)
        {
            this.numCols = count;
            return this;
        }

        public StatBuilder cells(int count)
        {
            this.numsCells = count;
            return this;
        }

        public StatBuilder entities(int count)
        {
            this.numEntities = count;
            return this;
        }

        public StatBuilder mappedCells(int count)
        {
            this.numMappedCells = count;
            return this;
        }

        public StatBuilder entitiesPerRow(List<Integer> entitiesPerRow)
        {
            this.entitiesPerRow = entitiesPerRow;
            return this;
        }

        public StatBuilder entitiesPerColumn(List<Integer> entitiesPerColumn)
        {
            this.entitiesPerColumn = entitiesPerColumn;
            return this;
        }

        public StatBuilder cellToEntityMatches(long count)
        {
            this.cellToEntityMatches = count;
            return this;
        }

        public StatBuilder entities(Set<String> entities)
        {
            this.entities = entities;
            return this;
        }

        public StatBuilder cellToEntityMatchesPerCol(List<Integer> cellToEntityMatches)
        {
            this.cellToEntityMatchesPerCol = cellToEntityMatches;
            return this;
        }

        public StatBuilder numericTableColumns(List<Boolean> numericTableColumns)
        {
            this.numericTableColumns = numericTableColumns;
            return this;
        }

        public Stats finish()
        {
            return new Stats(this.numRows, this.numCols, this.numsCells, this.numEntities, this.numMappedCells,
                    this.entitiesPerRow, this.entitiesPerColumn, this.cellToEntityMatchesPerCol,
                    this.entities, this.numericTableColumns, this.cellToEntityMatches);
        }
    }

    public static StatBuilder build()
    {
        return new StatBuilder();
    }

    private Stats(int rows, int columns, int cells, int entities, int mappedCells,
                  List<Integer> entitiesPerRow, List<Integer> entitiesPerColumn,
                  List<Integer> cellToEntityMatchesPerCol, Set<String> entitySet,
                  List<Boolean> numericTableColumns, long cellToEntityMatches)
    {
        this.numRows = rows;
        this.numCols = columns;
        this.numsCells = cells;
        this.numEntities = entities;
        this.numMappedCells = mappedCells;
        this.entitiesPerRow = entitiesPerRow;
        this.entitiesPerColumn = entitiesPerColumn;
        this.cellToEntityMatches = cellToEntityMatches;
        this.entities = entitySet;
        this.numericTableColumns = numericTableColumns;
        this.cellToEntityMatchesPerCol = cellToEntityMatchesPerCol;
    }

    public int rows()
    {
        return this.numRows;
    }

    public int columns()
    {
        return this.numCols;
    }

    public int cells()
    {
        return this.numsCells;
    }

    public int entities()
    {
        return this.numEntities;
    }

    public int mappedCells()
    {
        return this.numMappedCells;
    }

    public List<Integer> entitiesPerRow()
    {
        return this.entitiesPerRow;
    }

    public List<Integer> entitiesPerColumn()
    {
        return this.entitiesPerColumn;
    }

    public long cellToEntityMatches()
    {
        return this.cellToEntityMatches;
    }

    public Set<String> entitySet()
    {
        return this.entities;
    }

    public List<Integer> cellToEntityMatchesPerCol()
    {
        return this.cellToEntityMatchesPerCol;
    }

    public List<Boolean> numericTableColumns()
    {
        return this.numericTableColumns;
    }
}
