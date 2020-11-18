package dk.aau.cs.daisy.edao;


import dk.aau.cs.daisy.edao.commands.IndexTables;
import dk.aau.cs.daisy.edao.commands.SearchTables;
import picocli.CommandLine;

import java.io.File;



@CommandLine.Command(name = "thetis", version = "1.0-SNAPSHOT", subcommands = {
        IndexTables.class,
        SearchTables.class
})
public class App implements Runnable {

    /**
     * java -jar Thetis.1.0.jar  index|search  [options ..]
     */


    public void run() {
        System.err.println("This command should be called only via the subcommands index or search");
    }



    public static void main(String[] args) {
        // By implementing Runnable or Callable, parsing, error handling and handling user
        // requests for usage help or version help can be done with one line of code.

        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }
}