package com.thetis.search;

import com.thetis.structures.table.Table;

import java.util.function.Consumer;

public class DeferredQueryExecution extends QueryExecution
{
    private long durationMillis;
    private Thread execution;
    private Result result = null;

    public DeferredQueryExecution(AnalogousSearch search, long durationMillis)
    {
        super(search);
        this.durationMillis = durationMillis;
    }

    public void deferredExecute(Table<String> query, Consumer<Result> consume)
    {
        this.execution = new Thread(() -> {
            try
            {
                Thread.sleep(this.durationMillis);

                Result res = execute(query);
                consume.accept(result);
                this.result = res;
            }

            catch (InterruptedException ignored) {}
        });
        execution.start();
    }

    public Result getResult()
    {
        return this.result;
    }

    public void stopExecution()
    {
        this.execution.interrupt();
    }

    public void waitForCompletion()
    {
        try
        {
            this.execution.join();
        }

        catch (InterruptedException ignored) {}
    }
}
