package com.thetis.search;

import java.util.function.Function;

public abstract class Execution<Q, R>
{
    protected Function<Q, R> execution;

    protected Execution(Function<Q, R> execution)
    {
        this.execution = execution;
    }

    public Function<Q, R> getExecution()
    {
        return this.execution;
    }

    public abstract R execute(Q query);
}
