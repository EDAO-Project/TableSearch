package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.store.EmbeddingsIndex;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.table.Table;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BM25 extends AbstractSearch
{
    private long elapsedNs = -1;

    public BM25(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                EmbeddingsIndex<String> embeddingIdx)
    {
        super(linker, entityTable, entityTableLink, embeddingIdx);
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        int queryRows = query.rowCount();

        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build())
        {
            List<Pair<String, Double>> results = new ArrayList<>();

            for (int row = 0; row < queryRows; row++)
            {
                int queryColumns = query.getRow(row).size();

                for (int column = 0; column < queryColumns; column++)
                {
                    String entity = query.getRow(row).get(column);
                    SearchRequest request = new SearchRequest("bm25");
                    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                    sourceBuilder.query(QueryBuilders.termQuery("content", entity));
                    request.source(sourceBuilder);

                    RestHighLevelClient highLevelClient = new RestHighLevelClient(client);
                    SearchResponse response = highLevelClient.search(request);

                    SearchHits hits = response.getHits();

                    for (SearchHit hit : hits)
                    {
                        String table = hit.getSourceAsString();
                        double score = hit.getScore();
                        results.add(new Pair<>(table, score));
                    }
                }
            }

            this.elapsedNs = System.nanoTime() - start;
            return new Result(-1, results);
        }

        catch (IOException e)
        {
            return new Result(-1, List.of());
        }
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsedNs;
    }
}
