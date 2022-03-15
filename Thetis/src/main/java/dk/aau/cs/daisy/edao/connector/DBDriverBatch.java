package dk.aau.cs.daisy.edao.connector;

import java.util.List;

public interface DBDriverBatch<R, Q> extends DBDriver<R, Q>
{
    boolean batchInsert(List<String> iris, List<List<Float>> vectors);
}
