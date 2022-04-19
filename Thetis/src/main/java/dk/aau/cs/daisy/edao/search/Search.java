package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.structures.table.Table;

public interface Search
{
    Result search(Table<String> query);
    long elapsedNanoSeconds();
}
