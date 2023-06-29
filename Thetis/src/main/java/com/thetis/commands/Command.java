package com.thetis.commands;

import java.util.concurrent.Callable;

public abstract class Command implements Callable<Integer> {

    public abstract Integer call();

}
