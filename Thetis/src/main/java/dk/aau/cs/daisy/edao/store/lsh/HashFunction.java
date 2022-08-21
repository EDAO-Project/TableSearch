package dk.aau.cs.daisy.edao.store.lsh;

import java.io.Serializable;

public interface HashFunction extends Serializable
{
    /**
     * Computes hash code key
     * @param obj Object from which to compute hash code key
     * @param keys Number of keys
     * @return Hash code key, which must be greater than or equal to zero and less than <code>keys</code>
     */
    int hash(Object obj, int keys);
}
