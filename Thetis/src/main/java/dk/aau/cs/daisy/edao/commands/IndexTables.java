package dk.aau.cs.daisy.edao.commands;

import picocli.CommandLine;

import java.io.File;

@picocli.CommandLine.Command(name = "index", description = "creates the index for the specified set of tables")
public class IndexTables extends Command {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    /**
     * java -jar Thetis.1.0.jar  index  --type wikitables --tables  data/tables/wikitables --output data/index/wikitables
     */
    public enum TableType {
        WIKI, // WikiTables
        TT; // Tough Tables
    }


    @CommandLine.Option(names = { "-tt", "--table-type" }, description = "Table types: ${COMPLETION-CANDIDATES}")
    private TableType tableType = null;


    private File tableDir = null;
    @CommandLine.Option(names = { "-td", "--table-dir"}, paramLabel = "TABLE_DIR", description = "Directory containing tables")
    public void setTableDirectory(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the directory does not exists.", value));
        }

        if (!value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the path does not point to a directory.", value));
        }
        tableDir = value;
    }


    private File outputDir = null;
    @CommandLine.Option(names = { "-od", "--output-dir" }, paramLabel = "OUT_DIR", description = "Directory where to save the index")
    public void setOutputDirectory(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the directory does not exists.", value));
        }

        if (!value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the path does not point to a directory.", value));
        }

        if (!value.canWrite()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the directory is not writable.", value));
        }

        outputDir = value;
    }





    @Override
    public Integer call() {
        System.out.println("IndexTables command run: not implemented");



        return 23;
    }


}
