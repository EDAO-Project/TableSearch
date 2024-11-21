package com.thetis.search;

import com.thetis.structures.Pair;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Container of top-K search result in sorted descending order
 */
public class Result implements Comparable<Result>
{
    private int k, size;
    private List<Pair<String, Double>> tableScores;

    public Result(int k, List<Pair<String, Double>> tableScores)
    {
        this.k = k;
        this.size = Math.min(k, tableScores.size());
        this.tableScores = tableScores;
    }

    public Result(int k, Pair<String, Double> ... tableScores)
    {
        this(k, List.of(tableScores));
    }

    public int getK()
    {
        return this.k;
    }

    public void setK(int k)
    {
        this.k = k;
    }

    public int getSize()
    {
        return this.size;
    }

    public Iterator<Pair<String, Double>> getResults()
    {
        this.tableScores.sort((e1, e2) -> e2.getSecond().compareTo(e1.getSecond()));

        if (this.tableScores.size() < this.k + 1)
            return this.tableScores.iterator();

        return this.tableScores.subList(0, this.k).iterator();
    }

    public Set<Pair<String, Double>> getResultSet()
    {
        return new HashSet<>(this.tableScores);
    }

    @Override
    public int compareTo(Result other)
    {
        if (this.k < other.k)
        {
            return -1;
        }

        else if (this.k > other.k)
        {
            return 1;
        }

        return Double.compare(this.tableScores.get(0).getSecond(), other.tableScores.get(0).getSecond());
    }
}
