package com.thetis.loader.progressive;

import com.thetis.loader.IndexIO;

import java.nio.file.Path;

public interface ProgressiveIndexIO extends IndexIO
{
    boolean addTable(Path tablePath);
    void waitForCompletion();
    void pauseIndexing();
    void continueIndexing();
}
