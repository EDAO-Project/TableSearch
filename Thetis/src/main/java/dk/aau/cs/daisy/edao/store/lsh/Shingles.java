package dk.aau.cs.daisy.edao.store.lsh;

import java.util.List;
import java.util.Set;

public interface Shingles
{
    Set<List<String>> shingles();
}
