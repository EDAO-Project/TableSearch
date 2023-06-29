package com.thetis.store;

public interface Linker<F, T>
{
    T mapTo(F from);
    F mapFrom(T to);
    void addMapping(F from, T to);
    void clear();
}
