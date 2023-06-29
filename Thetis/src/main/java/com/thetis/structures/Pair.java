package com.thetis.structures;

import java.io.Serializable;
import java.util.Objects;

/**
 * Pair structure of two objects of any comparable type
 * @param <F> Comparable type of first object
 * @param <S> Comparable type of second object
 */
public class Pair<F extends Comparable, S extends Comparable> implements Serializable, Comparable<Pair<F, S>> {

    private final F first;
    private final S second;

    public Pair(F first, S second) {
        if(first == null){
            throw new NullPointerException("Null value for Pair.first is not allowed");
        }
        if(second == null){
            throw new NullPointerException("Null value for Pair.second is not allowed");
        }
        this.first = first;
        this.second = second;
    }

    public F getFirst() {
        return this.first;
    }

    public S getSecond() {
        return this.second;
    }

    @Override
    public boolean equals(Object other){
        if(other == null){
            return  false;
        }

        if (other == this){
            return true;
        }


        if (!(other instanceof Pair)){
            return false;
        }

        Pair<?,?> m = (Pair<?,?>) other;

        try {
            F _first = (F) m.getFirst();
            S _second = (S) m.getSecond();
            return Objects.equals(this.first, _first) && Objects.equals(this.second, _second);
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }

    }

    public int hashCode(){
        return this.first.hashCode() + 113*this.second.hashCode();
    }

    @Override
    public int compareTo(Pair<F, S> other)
    {
        if (this.first.compareTo(other.getFirst()) == 0)
            return this.second.compareTo(other.getSecond());

        return this.first.compareTo(other.getFirst());
    }
}
