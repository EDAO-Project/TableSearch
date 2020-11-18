package dk.aau.cs.daisy.edao.commands;


@picocli.CommandLine.Command(name = "search", description = "searched the index for tables matching the input tuples")
public class SearchTables extends Command {
    @Override
    public Integer call() {
        System.out.println("SearchTables command run: not implemented");
        return 1;
    }
}
