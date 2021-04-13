package dk.aau.cs.daisy.edao.utilities;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Files;

import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import dk.aau.cs.daisy.edao.tables.JsonTable;

public class utils {

    /**
     * Returns the JsonTable from a path to the json file
     * @param path: Path to the Json file corresponding to a table
     * @return a JsonTable object if table from path read succesfully. Otherwise returns an empty JsonTable
    */
    public static JsonTable getTableFromPath(Path path) {
        JsonTable table = new JsonTable();

        // Tries to parse the JSON file, it fails if file not found or JSON is not well formatted
        TypeAdapter<JsonTable> strictGsonObjectAdapter = new Gson().getAdapter(JsonTable.class);
        try (JsonReader reader = new JsonReader(new FileReader(path.toFile()))) {
            table = strictGsonObjectAdapter.read(reader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // We check if all the required json attributes are set
        if(table == null || table._id  == null || table.rows == null) {
            System.err.println("Failed to parse '"+path.toString()+"'");
            try {
                System.err.println(Files.readString(path));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return table;
    }


    /**
     * Returns the average vector given a list of vectors
     */
    public static List<Double> getVectorAverage(List<List<Double>> vecList) {
        // Initialize the avgVec to a vector of zeroes
        List<Double> avgVec = new ArrayList<>();
        for (Integer i=0;i<vecList.get(0).size(); i++) {
            avgVec.add(0.0);
        }

        // Construct sum
        for (List<Double> vec : vecList) {
            for (Integer i=0; i<vec.size(); i++) {
                avgVec.set(i, avgVec.get(i) + vec.get(i));
            }
        } 

        // Ge the Average
        for (Integer i=0; i<avgVec.size(); i++) {
            avgVec.set(i, avgVec.get(i) / vecList.size());
        }

        return avgVec;
    }

    /**
     * Returns the cosine similarity between two lists
     */
    public static double cosineSimilarity(List<Double> vectorA, List<Double> vectorB) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            dotProduct += vectorA.get(i) * vectorB.get(i);
            normA += Math.pow(vectorA.get(i), 2);
            normB += Math.pow(vectorB.get(i), 2);
        }

        // Handle division by zero
        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * Returns the Euclidian Distance between two lists
     */
    public static double euclideanDistance(List<Double> vectorA, List<Double> vectorB) {
        double Sum = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            Sum = Sum + Math.pow((vectorA.get(i) - vectorB.get(i)), 2.0);
        }
        return Math.sqrt(Sum);
    }
}
