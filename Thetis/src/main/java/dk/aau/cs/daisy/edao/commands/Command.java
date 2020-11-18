package dk.aau.cs.daisy.edao.commands;

import java.util.concurrent.Callable;

public abstract class Command implements Callable<Integer> {

    public abstract Integer call();

}
