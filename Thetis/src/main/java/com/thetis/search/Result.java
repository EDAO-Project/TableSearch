package com.thetis.search;

import com.thetis.structures.Pair;

import java.util.Iterator;
import java.util.List;

/**
 * Container of top-K search result in sorted descending order
 */
public class Result
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

    public int getSize()
    {
        return this.size;
    }

    public Iterator<Pair<String, Double>> getResults()
    {
        this.tableScores.sort((e1, e2) -> {
            if (e1.getSecond().equals(e2.getSecond()))
                return 0;

            return e1.getSecond() > e2.getSecond() ? -1 : 1;
        });

        if (this.tableScores.size() < this.k + 1)
            return this.tableScores.iterator();

        return this.tableScores.subList(0, this.k).iterator();
    }
}
