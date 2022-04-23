package dk.aau.cs.daisy.edao.utilities;

import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Files;

import java.util.*;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import dk.aau.cs.daisy.edao.commands.parser.EmbeddingsParser;
import dk.aau.cs.daisy.edao.commands.parser.Parser;
import dk.aau.cs.daisy.edao.similarity.CosineSimilarity;
import dk.aau.cs.daisy.edao.tables.JsonTable;

public class Utils {

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
    public static List<Double> getAverageVector(List<List<Double>> vecList) {
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

        // Get the Average
        for (Integer i=0; i<avgVec.size(); i++) {
            avgVec.set(i, avgVec.get(i) / vecList.size());
        }

        return avgVec;
    }

    /**
     * Returns the average of the vector
     */
    public static Double getAverageOfVector(List<Double> vec) {
        Double sum = 0.0;
        for (Double val : vec) {
            sum += val;
        }
        return sum / ((double)vec.size());
    }

    /**
     * Returns a list with the maximum value for each column in `arr`.
     * Note that `arr` is a 2D list of doubles
     */
    public static List<Double> getMaxPerColumnVector(List<List<Double>> arr) {
        Integer numColumns = arr.get(0).size();
        List<Double> maxColumnVec = new ArrayList<Double>(Collections.nCopies(numColumns, 0.0));

        for (Integer rowNum=0; rowNum<arr.size(); rowNum++) {
            for (Integer colNum=0; colNum<numColumns; colNum++) {
                if (arr.get(rowNum).get(colNum) > maxColumnVec.get(colNum)) {
                    maxColumnVec.set(colNum, arr.get(rowNum).get(colNum));
                }
            }
        } 

        return maxColumnVec;
    }


    /**
     * Returns the cosine similarity between two lists
     */
    public static double cosineSimilarity(List<Double> vectorA, List<Double> vectorB) {
        return CosineSimilarity.make(vectorA, vectorB).similarity();
    }

    /**
     * Returns the weighted Euclidian Distance between two lists
     * 
     * Assumes that the sizes of `vectorA`, `vectorB` and `weightVector` are all the same
     */
    public static double euclideanDistance(List<Double> vectorA, List<Double> vectorB, List<Double> weightVector) {
        double Sum = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            Sum = Sum + (Math.pow((vectorA.get(i) - vectorB.get(i)), 2.0)) * weightVector.get(i);
        }
        return Math.sqrt(Sum);
    }

    /**
     * Given a list of positive doubles, return a normalized list that sums to 1
     */
    public static List<Double> normalizeVector(List<Double> vec) {
        List<Double> normVec = new ArrayList<>(Collections.nCopies(vec.size(), 0.0));
        Double sum = 0.0;
        for (Double val : vec) {
            sum += val;
        }

        for (Integer i=0; i < vec.size(); i++) {
            normVec.set(i, vec.get(i) / sum);
        }
        return normVec;
    }

    /**
     * Returns the Hadamard product (i.e element-wise) between two vectors 
     */
    public static List<Double> hadamardProduct(List<Double> vectorA, List<Double> vectorB) {
        List<Double> returnVector = new ArrayList<>(Collections.nCopies(vectorA.size(), 0.0));

        for (Integer i=0; i < vectorA.size(); i++) {
            returnVector.set(i, vectorA.get(i) * vectorB.get(i));
        }
        return returnVector;
    }

    public static Parser getEmbeddingsParser(String content, char delimiter)
    {
        return new EmbeddingsParser(content, delimiter);
    }

    /**
     * Computes the logarithm of `val` in base 2
     */
    public static double log2(double val) {
        return Math.log(val) / Math.log(2);
    }
}