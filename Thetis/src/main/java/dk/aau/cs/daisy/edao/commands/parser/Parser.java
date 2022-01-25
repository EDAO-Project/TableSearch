package dk.aau.cs.daisy.edao.commands.parser;

import java.util.Iterator;

public interface Parser<T> extends Iterator<T>
{
    String nextLexeme();
    void reverse(int count);
}
