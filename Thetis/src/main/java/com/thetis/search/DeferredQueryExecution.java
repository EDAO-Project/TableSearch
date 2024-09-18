package com.thetis.search;

import com.thetis.structures.table.Table;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class DeferredQueryExecution extends QueryExecution
{
    private long durationMillis;
    private Future<Result> future;
    private Result result = null;

    public DeferredQueryExecution(AnalogousSearch search, long durationMillis)
    {
        super(search);
        this.durationMillis = durationMillis;
    }

    // TODO: Check if the result set is actually different, as the instance of AnalogousSearch might not have references to the indexes and therefore not access to the most recent version of these indexes
    public void deferredExecute(Table<String> query, Consumer<Result> consume)
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        this.future = executor.submit(() -> {
            try
            {
                Thread.sleep(this.durationMillis);

                Result result = execute(query);
                consume.accept(result);
                return result;
            }

            catch (InterruptedException ignored)
            {
                return null;
            }
        });

        try
        {
            this.result = this.future.get();
        }

        catch (InterruptedException | ExecutionException ignored) {}
    }

    public Result getResult()
    {
        return this.result;
    }
}
