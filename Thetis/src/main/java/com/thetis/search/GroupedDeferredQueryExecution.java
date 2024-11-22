package com.thetis.search;

import com.thetis.structures.table.Table;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public class GroupedDeferredQueryExecution extends DeferredQueryExecution
{
    private final List<Table<String>> queries = new ArrayList<>();
    private final List<Result> results = new ArrayList<>();
    private final Object lock = new Object();
    private boolean finished = false;

    public GroupedDeferredQueryExecution(AnalogousSearch search, long durationMillis)
    {
        super(search, durationMillis);
    }

    public GroupedDeferredQueryExecution(AnalogousSearch search, Predicate<Void> predicate)
    {
        super(search, predicate);
    }

    public GroupedDeferredQueryExecution(AnalogousSearch search, long durationMillis, Predicate<Void> predicate)
    {
        super(search, durationMillis, predicate);
    }

    public void clearQueries()
    {
        synchronized (this.lock)
        {
            this.queries.clear();
        }
    }

    public void addQueryResult(Table<String> query, Result result)
    {
        synchronized (this.lock)
        {
            this.queries.add(query);
            this.results.add(result);
        }
    }

    public void deferredExecute(BiConsumer<List<Result>, List<Result>> consume)
    {
        if (this.finished)
        {
            throw new IllegalStateException("Execution of queries has already been performed");
        }

        super.execution = new Thread(() -> {
            try
            {
                List<Result> newResults = new ArrayList<>();
                defer();

                synchronized (this.lock)
                {
                    for (Table<String> query : this.queries)
                    {
                        Result newResult = execute(query);
                        newResults.add(newResult);
                    }
                }

                consume.accept(this.results, newResults);
                this.finished = true;
            }

            catch (InterruptedException ignored) {}
        });
        super.execution.start();
    }

    public boolean isFinished()
    {
        return this.finished;
    }
}
