package com.thetis.search;

import com.thetis.commands.parser.TableParser;
import com.thetis.loader.FileRetriever;
import com.thetis.structures.table.Table;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;

public class QueryRetriever extends FileRetriever
{
    public QueryRetriever(File queryDirectory)
    {
        super(queryDirectory);
    }

    public Pair<File, Table<String>> nextQuery()
    {
        File queryFile = next();
        Table<String> queryTable = null;

        while (queryTable == null)
        {
            queryTable = TableParser.toTable(queryFile);
        }

        return Pair.of(queryFile, queryTable);
    }
}
