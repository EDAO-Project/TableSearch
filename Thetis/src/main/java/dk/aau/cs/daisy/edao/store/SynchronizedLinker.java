package dk.aau.cs.daisy.edao.store;

import java.io.Serializable;

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

    public Linker<F, T> getLinker()
    {
        return this.linker;
    }
}
