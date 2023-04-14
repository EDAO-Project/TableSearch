package dk.aau.cs.daisy.edao.store.lsh;

import java.util.List;

public abstract class Distance<E>
{
    private List<E> vec1, vec2;

    protected Distance(List<E> v1, List<E> v2)
    {
        this.vec1 = v1;
        this.vec2 = v2;
    }

    protected List<E> getVec1()
    {
        return this.vec1;
    }

    protected List<E> getVec2()
    {
        return this.vec2;
    }

    public double distance()
    {
        return measureDistance();
    }

    protected abstract double measureDistance();
}
