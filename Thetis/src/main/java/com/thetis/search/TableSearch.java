package com.thetis.search;

import com.thetis.structures.table.Table;

/**
 * Any class conducting table search should implement this interface.
 * It ensures results are captures correctly, generically, and optimally.
 */
public interface TableSearch
{
    Result search(Table<String> query);
    long elapsedNanoSeconds();
}
