package com.thetis.utilities;

import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Files;

import java.util.*;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.thetis.commands.parser.EmbeddingsParser;
import com.thetis.commands.parser.Parser;
import com.thetis.similarity.CosineSimilarity;
import com.thetis.tables.JsonTable;
import com.thetis.structures.table.Table;

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
     * Returns the average vector given a row of vector scores
     */
    public static List<Double> getAverageVector(Table.Row<List<Double>> row) {
        List<Double> avgVec = new ArrayList<>();

        for (int i = 0; i < row.size(); i++) {
            avgVec.add(0.0);
        }

        for (int i = 0; i < row.size(); i++) {
            for (int j = 0; j < row.get(i).size(); j++) {
                avgVec.set(i, avgVec.get(i) + row.get(i).get(j));
            }
        }

        for (int i = 0; i < avgVec.size(); i++) {
            avgVec.set(i, avgVec.get(i) / row.get(i).size());
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

    public static List<Double> averageVector(List<List<Double>> vec) {
        if (vec.isEmpty()) {
            return List.of();
        }

        List<Double> avgVec = new ArrayList<>(Collections.nCopies(vec.get(0).size(), 0.0));

        for (int vector = 0; vector < vec.size(); vector++) {
            if (vec.get(vector).size() != avgVec.size()) {
                throw new IllegalArgumentException("Dimension mis-match when computing average vector");
            }

            for (int row = 0; row < vec.get(vector).size(); row++) {
                avgVec.set(row, avgVec.get(row) + vec.get(vector).get(row));
            }
        }

        for (int i = 0; i < avgVec.size(); i++)
        {
            avgVec.set(i, avgVec.get(i) / vec.size());
        }

        return avgVec.stream().map(val -> val / vec.size()).collect(Collectors.toList());
    }

    /**
     * Returns a list with the maximum value for each column in `row`.
     * Note that `row` is a 2D list of doubles from tables of scores
     */
    public static List<Double> getMaxPerColumnVector(Table.Row<List<Double>> row) {
        List<Double> maxColumnVec = new ArrayList<>(Collections.nCopies(row.size(), 0.0));

        for (int column = 0; column < row.size(); column++) {
            int cellCount = row.get(column).size();

            for (int cellDim = 0; cellDim < cellCount; cellDim++) {
                if (row.get(column).get(cellDim) > maxColumnVec.get(column)) {
                    maxColumnVec.set(column, row.get(column).get(cellDim));
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
     * Returns the weighted Euclidean Distance between two lists
     * 
     * Assumes that the sizes of `vectorA`, `vectorB` and `weightVector` are all the same
     */
    public static double euclideanDistance(List<Double> vectorA, List<Double> vectorB, List<Double> weightVector) {
        double sum = 0.0;

        for (int i = 0; i < vectorA.size(); i++) {
            sum += Math.pow((vectorA.get(i) - vectorB.get(i)), 2.0) * weightVector.get(i);
        }

        return Math.sqrt(sum);
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