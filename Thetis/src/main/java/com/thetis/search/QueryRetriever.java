package com.thetis.search;

import com.thetis.commands.parser.TableParser;
import com.thetis.structures.table.Table;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class QueryRetriever implements Iterator<Pair<File, Table<String>>>
{
    private final File dir;

    public QueryRetriever(File queryDirectory)
    {
        this.dir = queryDirectory;
    }

    public File getDirectory()
    {
        return this.dir;
    }

    @Override
    public boolean hasNext()
    {
        return this.dir.listFiles().length > 0;
    }

    @Override
    public Pair<File, Table<String>> next()
    {
        while (!hasNext())
        {
            try
            {
                TimeUnit.SECONDS.sleep(2);
            }

            catch (InterruptedException ignored) {}
        }

        File queryFile = this.dir.listFiles()[0];
        Table<String> queryTable = null;

        while (queryTable == null)
        {
            queryTable = TableParser.toTable(queryFile);
        }

        return Pair.of(queryFile, queryTable);
    }
}
