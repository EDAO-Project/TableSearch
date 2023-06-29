package com.thetis.structures;

import java.io.Serializable;

/**
 * Most basic pair class of two element of any type
 * @param <F>
 * @param <S>
 */
public class PairNonComparable<F, S> implements Serializable
{
    private F first;
    private S second;

    public PairNonComparable(F first, S second)
    {
        this.first = first;
        this.second = second;
    }

    public F getFirst()
    {
        return this.first;
    }

    public S getSecond()
    {
        return this.second;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PairNonComparable))
        {
            return false;
        }

        PairNonComparable<?, ?> other = (PairNonComparable<?, ?>) o;
        return this.first.equals(other.getFirst()) && this.first.equals(other.getSecond());
    }
}
