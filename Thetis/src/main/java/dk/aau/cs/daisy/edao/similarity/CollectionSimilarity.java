package dk.aau.cs.daisy.edao.similarity;

import java.util.Collection;

public abstract class CollectionSimilarity<E> extends SimilarityBase
{
    protected Collection<E> l1, l2;

    public CollectionSimilarity(Collection<E> l1, Collection<E> l2)
    {
        this.l1 = l1;
        this.l2 = l2;
    }
}
