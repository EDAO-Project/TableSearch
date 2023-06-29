package com.thetis.search;

import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.structures.Id;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;
import com.thetis.system.Logger;
import com.thetis.utilities.Ppr;

import java.util.*;

/**
 * Debugging search class for searching using Personalized PageRank
 */
public class PPRSearch extends AbstractSearch
{
    private Neo4jEndpoint neo4j;

    private boolean weighted;
    private double threshold, particles;
    private int topK;
    private List<List<Double>> weights;

    private double elapsedTime = -1;

    public PPRSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, EmbeddingsIndex<String> embeddingsIndex,
                     Neo4jEndpoint neo4j, boolean weightedPPR, double minThreshold, double particles, int topK)
    {
        super(linker, entityTable, entityTableLink, embeddingsIndex);
        this.neo4j = neo4j;
        this.weighted = weightedPPR;
        this.threshold = minThreshold;
        this.particles = particles;
        this.topK = topK;
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        if (this.weighted)
        {
            this.weights = Ppr.getWeights(this.neo4j, query, getIDFMapping());
        }

        else
        {
            this.weights = Ppr.getUniformWeights(query);
        }

        Map<String, Double> tableScores = new HashMap<>();
        int rowCount = query.rowCount();
        long startTime = System.nanoTime();
        Logger.logNewLine(Logger.Level.INFO, "\n\nRunning PPR over the " + query.rowCount() + " provided Query Tuple(s)...");
        Logger.logNewLine(Logger.Level.INFO, "PPR Weights: " + this.weights);

        for (int row = 0; row < rowCount; row++)
        {
            Map<String, Double> nodeTableScores = this.neo4j.runPPR(query.getRow(row), this.weights.get(row), this.threshold,
                    this.particles, this.topK);

            for (String s : nodeTableScores.keySet())
            {
                if (!tableScores.containsKey(s))
                {
                    tableScores.put(s, nodeTableScores.get(s));
                }

                else
                {
                    tableScores.put(s, tableScores.get(s) + nodeTableScores.get(s));
                }
            }

            Logger.logNewLine(Logger.Level.INFO, "Finished computing PPR for tuple: " + row);
        }

        this.elapsedTime = (System.nanoTime() - startTime) / 1e9;
        Logger.logNewLine(Logger.Level.INFO, "\n\nFinished running PPR over the given Query Tuple(s)");
        Logger.logNewLine(Logger.Level.INFO, "Elapsed time: " + this.elapsedTime + " seconds\n");
        tableScores = sortByValue(tableScores);

        return new Result(this.topK, toScorePairs(tableScores));
    }

    private Map<String, Double> getIDFMapping()
    {
        Map<String, Double> entityToIDF = new HashMap<>();
        Iterator<Id> entityIter = getLinker().kgUriIds();

        while (entityIter.hasNext())
        {
            Id entity = entityIter.next();
            double idf = getEntityTable().find(entity).getIDF();
            entityToIDF.put(getLinker().kgUriLookup(entity), idf);
        }

        return entityToIDF;
    }

    private static Map<String, Double> sortByValue(Map<String, Double> hm)
    {
        Map<String, Double> temp = new LinkedHashMap<>();
        List<Map.Entry<String, Double> > list = new LinkedList<Map.Entry<String, Double> >(hm.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Double> >() {
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                int i = (o1.getValue()).compareTo(o2.getValue());
                if(i != 0) return -i;
                return i;
            }
        });

        for (Map.Entry<String, Double> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }

        return temp;
    }

    private static List<Pair<String, Double>> toScorePairs(Map<String, Double> scores)
    {
        List<Pair<String, Double>> scorePairs = new ArrayList<>(scores.size());

        for (Map.Entry<String, Double> entry : scores.entrySet())
        {
            scorePairs.add(new Pair<>(entry.getKey(), entry.getValue()));
        }

        return scorePairs;
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return (long) this.elapsedTime;
    }
}
