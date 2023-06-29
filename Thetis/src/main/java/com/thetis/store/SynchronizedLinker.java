package com.thetis.store;

import java.io.Serializable;

/**
 * Wrapper class for entity linker class
 * This class provides synchronization to linker classes
 * @param <F> Type of entity to map from
 * @param <T> Type of entity to map to
 */
public class SynchronizedLinker<F, T> implements Linker<F, T>, Serializable
{
    private Linker<F, T> linker;

    public static <From, To> SynchronizedLinker<From, To> wrap(Linker<From, To> linker)
    {
        return new SynchronizedLinker<>(linker);
    }

    public SynchronizedLinker(Linker<F, T> linker)
    {
        this.linker = linker;
    }

    @Override
    public synchronized T mapTo(F from)
    {
        return this.linker.mapTo(from);
    }

    @Override
    public synchronized F mapFrom(T to)
    {
        return this.linker.mapFrom(to);
    }
    @Override
    public synchronized void addMapping(F from, T to)
    {
        this.linker.addMapping(from, to);
    }

    @Override
    public void clear()
    {
        this.linker.clear();
    }

    public Linker<F, T> getLinker()
    {
        return this.linker;
    }
}
