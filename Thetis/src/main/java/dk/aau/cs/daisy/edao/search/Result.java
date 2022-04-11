package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.structures.Pair;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Result
{
    private int k, size;
    List<Pair<String, Double>> tableScores;

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
        Collections.sort(this.tableScores, (e1, e2) -> {
            if (e1.getSecond() == e2.getSecond())
                return 0;

            return e1.getSecond() > e2.getSecond() ? -1 : 1;
        });

        return this.tableScores.subList(0, this.k + 1).iterator();
    }
}
