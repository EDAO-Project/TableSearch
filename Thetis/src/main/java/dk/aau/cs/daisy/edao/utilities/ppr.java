package dk.aau.cs.daisy.edao.utilities;
import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;

import java.util.*;

public class ppr {
    
    /**
     * Returns a List<List<Double>> with the weight scores
     * @param connector: A Neo4jEndpoint to the graph database
     * @param queryEntities: A 2D list of the query tuples. Indexed by (tupleID, entityPosition) 
     * @param entityToIDF: A mapping of each entity to its IDF score
     * @return a List<List<Double>> with the respective weights for each query entity in the same order as in `queryEntities`
    */
    public static List<List<Double>> getWeights(Neo4jEndpoint connector, List<List<String>> queryEntities,
        Map<String, Double> entityToIDF) {
        List<List<Double>> weights = new ArrayList<>();

        // Compute the edge ratio scores
        List<List<Double>> edge_ratio_scores = getEdgeRatioScores(connector, queryEntities);

        // Compute the IDF ratio scores
        List<List<Double>> idf_ratio_scores = getIDFRatioScores(queryEntities, entityToIDF);
        

        // Parameters for the weights between the edge ratio scores and the idf ratio scores
        Double alpha1 = 0.5;
        Double alpha2 = 0.5;
        for (Integer i=0; i<queryEntities.size(); i++) {
            List<Double> weight_per_query_tuple = new ArrayList<>();
            for (Integer j=0; j<queryEntities.get(i).size(); j++){
                Double weight = alpha1 * edge_ratio_scores.get(i).get(j) + alpha2 * idf_ratio_scores.get(i).get(j);
                weight_per_query_tuple.add(weight);
            }
            weights.add(weight_per_query_tuple);
        }

        return weights;
    }

    /**
     * 
     * @param queryEntities: A 2D list of the query tuples. Indexed by (tupleID, entityPosition) 
     * @return a List<List<Double>> with uniform weights for each entity in the `queryEntities`. All weights are set to 1.0
    */
    public static List<List<Double>> getUniformWeights(List<List<String>> queryEntities) {
        List<List<Double>> weights = new ArrayList<>();
        for (Integer i=0; i<queryEntities.size(); i++) {
            List<Double> weightPerQueryTuple = new ArrayList<>();
            for (Integer j=0; j<queryEntities.get(i).size(); j++){
                Double weight = 1.0;
                weightPerQueryTuple.add(weight);
            }
            weights.add(weightPerQueryTuple);
        }
        return weights;
    }

    public static List<List<Double>> getEdgeRatioScores(Neo4jEndpoint connector, List<List<String>> queryEntities) {
        // TODO: Computing the number of edges is very slow (Use a constant for now, any better fix?)
        Long numNodes = connector.getNumNodes();
        // Integer numEdges = connector.getNumEdges();
        Long numEdges = 98900215L;
        Double meanEdgesPerNode = (double)numEdges / numNodes;

        // Compute edge ratio scores
        // (i.e., the ration of the number of edges of a node divided by the mean number of edges per node)
        List<List<Double>> edge_ratio_scores = new ArrayList<>();
        for (List<String> queryTuple :  queryEntities) {
            List<Double> edge_scores_per_q_tuple = new ArrayList<>();
            for (String queryNode : queryTuple) {
                // TODO: Currently the number of edges for query nodes is too high compared to the mean
                // so maybe scale the rations by a log factor instead?
                Double ratio = connector.getNumNeighbors(queryNode) / meanEdgesPerNode;

                if (ratio > 1.0) {
                    // Adjust the ratio by a log factor
                    ratio = 1 + Math.log10(ratio);
                }
                edge_scores_per_q_tuple.add(ratio);
            }
            edge_ratio_scores.add(edge_scores_per_q_tuple);
        }
        return edge_ratio_scores;
    }


    public static List<List<Double>> getIDFRatioScores(List<List<String>> queryEntities, Map<String, Double> entityToIDF) {
        // Compute IDF ratio scores
        List<List<Double>> idf_ratio_scores = new ArrayList<>();
        Double IDF_sum=0.0;
        Integer numEntities=0;

        for (List<String> queryTuple :  queryEntities) {
            List<Double> idf_scores_per_tuple = new ArrayList<>();
            for (String queryNode : queryTuple) {
                Double score = entityToIDF.get(queryNode);
                IDF_sum += score;
                numEntities+=1;
                idf_scores_per_tuple.add(score);
            }
            idf_ratio_scores.add(idf_scores_per_tuple);
        }
        Double mean_IDF= IDF_sum / numEntities;

        for (Integer i=0; i<idf_ratio_scores.size(); i++) {
            for (Integer j=0; j<idf_ratio_scores.get(i).size(); j++){
                idf_ratio_scores.get(i).set(j, idf_ratio_scores.get(i).get(j) / mean_IDF);
            }
        }

        return idf_ratio_scores;
    }


    /**
     * @return An updated `queryEntities` list with only one query tuple that includes all query entities
     * from all query tuples 
     * 
    */
    public static List<List<String>> combineQueryTuplesInSingleTuple(List<List<String>> queryEntities) {
        List<List<String>> newQueryEntities = new ArrayList<>();

        // Keep track of all unique entities seen in a set
        Set<String> entitiesSet = new HashSet<String>();
        for (List<String> queryTuple :  queryEntities) {
            for (String queryEntity : queryTuple) {
                entitiesSet.add(queryEntity);
            }
        }

        List<String> newQueryTuple = new ArrayList<String>(entitiesSet);
        newQueryEntities.add(newQueryTuple);

        return newQueryEntities;
    }


}
