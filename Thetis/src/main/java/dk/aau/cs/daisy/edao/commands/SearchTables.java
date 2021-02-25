package dk.aau.cs.daisy.edao.commands;


@picocli.CommandLine.Command(name = "search", description = "searched the index for tables matching the input tuples")
public class SearchTables extends Command {
    @Override
    public Integer call() {
        System.out.println("SearchTables command run: not implemented");
        return 1;


        /**
         *
         * comman in input takes a list of dbpedia entities  http://dbpedia.org/resource/Federica_pellegrini http://dbpedia.org/resource/Italy
         *
         * For each table /// for each table retrived with PPR
         *      for each row in table
         *          for each entity in query -> compute best cell match  http://dbpedia.org/resource/Federica_pellegrini -> entity in cell 4, http://dbpedia.org/resource/Italy -> entity in cell 2
         *
         *
         *
         *
         */



    }
}
