package com.thetis.loader.progressive;

import com.thetis.search.Result;
import com.thetis.structures.Pair;

import java.util.Set;

public abstract class ResultAdapter
{
    private final Set<Pair<Result, Result>> results;

    protected ResultAdapter(Set<Pair<Result, Result>> results)
    {
        this.results = results;
    }

    protected Set<Pair<Result, Result>> getResults()
    {
        return this.results;
    }
}
