package com.thetis.search;

import com.thetis.structures.table.Table;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class DeferredQueryExecution extends QueryExecution
{
    private long durationMillis = -1;
    private Predicate<Void> predicate;
    private Thread execution;
    private Result result = null;

    public DeferredQueryExecution(AnalogousSearch search, long durationMillis)
    {
        super(search);
        this.durationMillis = durationMillis;
        this.predicate = null;
    }

    public DeferredQueryExecution(AnalogousSearch search, Predicate<Void> predicate)
    {
        super(search);
        this.durationMillis = -1;
        this.predicate = predicate;
    }

    public DeferredQueryExecution(AnalogousSearch search, long durationMillis, Predicate<Void> predicate)
    {
        super(search);
        this.durationMillis = durationMillis;
        this.predicate = predicate;
    }

    public void deferredExecute(Table<String> query, Consumer<Result> consume)
    {
        this.execution = new Thread(() -> {
            try
            {
                if (this.predicate == null)
                {
                    deferredExecuteTime();
                }

                else
                {
                    deferredExecutePredicate();
                }

                Result res = execute(query);
                consume.accept(res);
                this.result = res;
            }

            catch (InterruptedException ignored) {}
        });
        execution.start();
    }

    private void deferredExecuteTime() throws InterruptedException
    {
        Thread.sleep(this.durationMillis);
    }

    private void deferredExecutePredicate() throws InterruptedException
    {
        long duration = this.durationMillis > 0 ? this.durationMillis : 1000;

        while (this.predicate.test(null))
        {
            Thread.sleep(duration);
        }
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
