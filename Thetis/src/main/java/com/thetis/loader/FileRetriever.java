package com.thetis.loader;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class FileRetriever implements Iterator<File>
{
    protected final File dir;

    public FileRetriever(File fileDirectory)
    {
        this.dir = fileDirectory;
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
    public File next()
    {
        while (!hasNext())
        {
            try
            {
                TimeUnit.SECONDS.sleep(1);
            }

            catch (InterruptedException ignored) {}
        }

        return this.dir.listFiles()[0];
    }
}
