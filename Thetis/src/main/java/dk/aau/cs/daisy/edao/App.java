package dk.aau.cs.daisy.edao;

import dk.aau.cs.daisy.edao.commands.IndexTables;
import dk.aau.cs.daisy.edao.commands.SearchTables;
import dk.aau.cs.daisy.edao.commands.Web;

import picocli.CommandLine;

import java.io.File;
import java.nio.file.*;
// import java.nio.file.Path;

import static spark.Spark.*;
import spark.ModelAndView;
import spark.template.mustache.MustacheTemplateEngine;

import java.util.*;

import com.google.gson.*;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;

import java.io.*;

import org.apache.commons.cli.*;

@CommandLine.Command(name = "thetis", version = "1.0-SNAPSHOT", subcommands = {
        IndexTables.class,
        SearchTables.class,
        Web.class,
})
public class App implements Runnable {

    /**
     * java -jar Thetis.1.0.jar  index|search  [options ..]
     */

    public void run() {
        System.err.println("This command should be called only via the subcommands index or search");
    }

    public static String render(Map<String, Object> model, String templatePath) {
        return new MustacheTemplateEngine().render(new ModelAndView(model, templatePath));
    }

    public static void main(String[] args) {
        // By implementing Runnable or Callable, parsing, error handling and handling user
        // requests for usage help or version help can be done with one line of code.

        String arg_0 = args[0];

        if (arg_0.equals("web")) {
            System.out.println("Initializing web interface...");

            // Web interface commandline parsing
            org.apache.commons.cli.CommandLine cmd = webInterfaceCommandLine(args);
            
            String mode = cmd.getOptionValue("mode");
            String tableDir = cmd.getOptionValue("table-dir");
            String outputDir = cmd.getOptionValue("output-dir");
            System.out.println("Mode: " + mode);
            System.out.println("Table Directory: " + tableDir);
            System.out.println("Output Directory: " + outputDir + "\n\n");

            staticFiles.location("/public");

            // Index page
            get("/", (req, res) -> {
                Map<String, Object> model = new HashMap<>();
                model.put("queryString", "");
                return render(model, "index.html");
            });

            // Post when user clicks the query submit button
            post("/query_submit", (req, res) -> {
                System.out.println("In /query_submit route...");

                String rawQueryString = req.queryParams("query");
                String queryString = "{\"queries\": [" + rawQueryString + "]}";
                System.out.println("Input Query: " + queryString);

                // Convert the query string into an appropriate JSON object to be used for querying
                try {
                    Writer writer = new FileWriter("../data/queries/user_query.json");
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();

                    JsonElement jelem = gson.fromJson(queryString, JsonElement.class);
                    JsonObject jobj = jelem.getAsJsonObject();

                    System.out.println("Updated Input Query: " + jobj);

                    gson.toJson(jobj, writer);
                    writer.close();
                }
                catch (IOException i) {
                    i.printStackTrace();
                }

                // TODO: Check if the user entered dbpedia entries are valid


                // Run search command (Currently set to use the baseline)
                // String[] searchCommand = new String[]{
                //     "search",
                //     "--search-mode", "analogous",
                //     "--hashmap-dir", "../data/index/small_test/",
                //     "--query-file", "../data/queries/user_query.json",
                //     "--table-dir", "/data/tables/wikitables/small_test/",
                //     "--output-dir", "/data/index/small_test/search_output/"
                // };

                String[] searchCommand = new String[]{
                    "search",
                    "--search-mode", mode,
                    "--hashmap-dir", "../data/index/small_test/",
                    "--query-file", "../data/queries/user_query.json",
                    "--table-dir", tableDir,
                    "--output-dir", outputDir
                };

                int exitCode = new CommandLine(new App()).execute(searchCommand);
                if (exitCode == 1) {
                    System.out.println("Successful Search Completed!");
                }
                else {
                    // Throw error
                }

                List<Object> tableToScore = new ArrayList<>();
                // Read the scores output and populate `tableToScore`
                try {
                    Gson gson = new Gson();
                    Path scoresFilePath = Paths.get(outputDir + "filenameToScore.json");
                    Reader reader = Files.newBufferedReader(scoresFilePath);
            
                    // convert JSON file to a hashmap and then extract the list of queries
                    Type type = new TypeToken<HashMap<String, Object>>(){}.getType();
                    Map<String, List<String>> tableToScoreFull = gson.fromJson(reader, type); 
                    reader.close();

                    // If there are more than 20 tables in `tableToScoreFull` choose only the top-20 results to show
                    Integer k = 20;
                    if (tableToScoreFull.get("scores").size() < 20) {
                        k = tableToScoreFull.get("scores").size();
                    }
                    List<String> scores = tableToScoreFull.get("scores");
                    for (Integer i=0; i<k; i++) {
                        tableToScore.add(scores.get(i));
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }

                Map<String, Object> model = new HashMap<>();
                model.put("tableToScore", tableToScore);
                model.put("show_table", true);
                model.put("queryString", rawQueryString);
                return render(model, "index.html");
            });
        }
        else {
            int exitCode = new CommandLine(new App()).execute(args);
            System.exit(exitCode);
        }
    }

    private static org.apache.commons.cli.CommandLine webInterfaceCommandLine(String[] args) {
        Options options = new Options();

        // Specifies if we run baseline or PPR
        Option input = new Option("m", "mode", true, "Must be one of {baseline, PPR}");
        input.setRequired(true);
        options.addOption(input);

        input = new Option("td", "table-dir", true, "Path to the tables directory");
        input.setRequired(true);
        options.addOption(input);

        input = new Option("od", "output-dir", true, "Path to the output directory");
        input.setRequired(true);
        options.addOption(input);

        org.apache.commons.cli.CommandLine cmd = new org.apache.commons.cli.CommandLine(){};

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
        }
        catch (Exception e) {
            e.printStackTrace();
        }  

        return cmd;
    }
}