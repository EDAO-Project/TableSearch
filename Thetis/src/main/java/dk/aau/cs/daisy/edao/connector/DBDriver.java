package dk.aau.cs.daisy.edao.connector;

public interface DBDriver<R, Q>
{
    R select(Q query);
    boolean update(Q query);
    boolean updateSchema(Q query);
    boolean close();
    boolean drop(Q query);
}
