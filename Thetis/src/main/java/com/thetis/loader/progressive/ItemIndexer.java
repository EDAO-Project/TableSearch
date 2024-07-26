package com.thetis.loader.progressive;

public interface ItemIndexer<I>
{
    void index(String id, int row, I item);
}
