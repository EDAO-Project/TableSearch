package com.thetis.loader;

import java.util.List;
import java.util.Set;

public class Stats
{
    private int numRows, numCols, numsCells, numEntities, numMappedCells, entityMappedRows;
    private long cellToEntityMatches;
    private List<Integer> entitiesPerRow, entitiesPerColumn, cellToEntityMatchesPerCol;
    List<Boolean> numericTableColumns;
    private Set<String> entities;
    private List<List<String>> tupleQueryAlignment;
    private double fractionOfEntityMappedRows;
    private List<Double> queryRowScores;
    private List<List<Double>> queryRowVectors;

    public static class StatBuilder
    {
        private int numRows = -1, numCols = -1, numsCells = -1, numEntities = -1, numMappedCells = -1, entityMappedRows = -1;
        private long cellToEntityMatches = -1;
        private List<Integer> entitiesPerRow = null, entitiesPerColumn = null, cellToEntityMatchesPerCol = null;
        List<Boolean> numericTableColumns = null;
        private Set<String> entities = null;
        private List<List<String>> tupleQueryAlignment = null;
        private double fractionOfEntityMappedRows = -1;
        private List<Double> queryRowScores = null;
        List<List<Double>> queryRowVectors = null;

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

        public StatBuilder tupleQueryAlignment(List<List<String>> alignment)
        {
            this.tupleQueryAlignment = alignment;
            return this;
        }

        public StatBuilder entityMappedRows(int count)
        {
            this.entityMappedRows = count;
            return this;
        }

        public StatBuilder fractionOfEntityMappedRows(double fraction)
        {
            this.fractionOfEntityMappedRows = fraction;
            return this;
        }

        public StatBuilder queryRowScores(List<Double> scores)
        {
            this.queryRowScores = scores;
            return this;
        }

        public StatBuilder queryRowVectors(List<List<Double>> vectors)
        {
            this.queryRowVectors = vectors;
            return this;
        }

        public Stats finish()
        {
            return new Stats(this.numRows, this.numCols, this.numsCells, this.numEntities, this.numMappedCells,
                    this.entitiesPerRow, this.entitiesPerColumn, this.cellToEntityMatchesPerCol,
                    this.entities, this.numericTableColumns, this.cellToEntityMatches, this.tupleQueryAlignment,
                    this.entityMappedRows, this.fractionOfEntityMappedRows, this.queryRowScores, this.queryRowVectors);
        }
    }

    public static StatBuilder build()
    {
        return new StatBuilder();
    }

    private Stats(int rows, int columns, int cells, int entities, int mappedCells,
                  List<Integer> entitiesPerRow, List<Integer> entitiesPerColumn,
                  List<Integer> cellToEntityMatchesPerCol, Set<String> entitySet,
                  List<Boolean> numericTableColumns, long cellToEntityMatches,
                  List<List<String>> tupleQueryAlignment, int entityMappedRows,
                  double fractionOfEntityMappedRows, List<Double> queryRowScores,
                  List<List<Double>> queryRowVectors)
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
        this.tupleQueryAlignment = tupleQueryAlignment;
        this.entityMappedRows = entityMappedRows;
        this.fractionOfEntityMappedRows = fractionOfEntityMappedRows;
        this.queryRowScores = queryRowScores;
        this.queryRowVectors = queryRowVectors;
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

    public List<List<String>> tupleQueryAlignment()
    {
        return this.tupleQueryAlignment;
    }

    public int entityMappedRows()
    {
        return this.entityMappedRows;
    }

    public double fractionOfEntityMappedRows()
    {
        return this.fractionOfEntityMappedRows;
    }

    public List<Double> queryRowScores()
    {
        return this.queryRowScores;
    }

    public List<List<Double>> queryRowVectors()
    {
        return this.queryRowVectors;
    }
}
