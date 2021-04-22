package dk.aau.cs.daisy.edao;


import dk.aau.cs.daisy.edao.commands.IndexTables;
import dk.aau.cs.daisy.edao.commands.SearchTables;
import dk.aau.cs.daisy.edao.commands.Web;

import picocli.CommandLine;

import java.io.File;

import static spark.Spark.*;
import spark.ModelAndView;
import spark.template.mustache.MustacheTemplateEngine;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

import com.google.gson.*;
import java.io.*;



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
        System.out.println(arg_0);

        if (arg_0.equals("web")) {
            System.out.println("Initaliazing web interface...");

            // Index page
            get("/", (req, res) -> {
                Map<String, Object> model = new HashMap<>();
                return render(model, "index.html");
            });

            // Post when user clicks the query submit button
            post("/query_submit", (req, res) -> {
                System.out.println("In /query_submit route...");

                String queryString = req.queryParams("query");
                queryString = "{\"queries\": [" + queryString + "]}";
                System.out.println("Input Query: " + queryString);

                // Convert the query string into an appropriate JSON object to be used for querying
                try {
                    Writer writer = new FileWriter("../data/queries/test.json");
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

                Map<String, Object> model = new HashMap<>();
                model.put("result", "table1");
                return render(model, "index.html");
            });
        }
        else {
            int exitCode = new CommandLine(new App()).execute(args);
            System.exit(exitCode);
        }
    }
}


// import static spark.Spark.*;
// import spark.ModelAndView;
// import spark.template.mustache.MustacheTemplateEngine;

// import java.util.HashMap;
// import java.util.Map;
// import java.util.Arrays;

// import com.google.gson.*;
// import java.io.*;


// public class App {

//     public static String render(Map<String, Object> model, String templatePath) {
//         return new MustacheTemplateEngine().render(new ModelAndView(model, templatePath));
//     }

//     public static void main(String[] args) {

//         // Index page
//         get("/", (req, res) -> {
//             Map<String, Object> model = new HashMap<>();
//             return render(model, "index.html");
//         });

//         // Post when user clicks the query submit button
//         post("/query_submit", (req, res) -> {
//             System.out.println("In /query_submit route...");

//             String queryString = req.queryParams("query");
//             queryString = "{\"queries\": [" + queryString + "]}";
//             System.out.println("Input Query: " + queryString);

//             // Convert the query string into an appropriate JSON object to be used for querying
//             try {
//                 Writer writer = new FileWriter("../data/queries/test.json");
//                 Gson gson = new GsonBuilder().setPrettyPrinting().create();

//                 JsonElement jelem = gson.fromJson(queryString, JsonElement.class);
//                 JsonObject jobj = jelem.getAsJsonObject();

//                 System.out.println("Updated Input Query: " + jobj);

//                 gson.toJson(jobj, writer);
//                 writer.close();
//             }
//             catch (IOException i) {
//                 i.printStackTrace();
//             }

//             Map<String, Object> model = new HashMap<>();
//             model.put("result", "table1");
//             return render(model, "index.html");
//         });
//     }
// }