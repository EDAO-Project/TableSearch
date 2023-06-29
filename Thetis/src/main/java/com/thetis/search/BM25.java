package com.thetis.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

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

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build())
        {
            List<Pair<String, Double>> results = new ArrayList<>();
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            for (int row = 0; row < queryRows; row++)
            {
                int queryColumns = query.getRow(row).size();

                for (int column = 0; column < queryColumns; column++)
                {
                    String uri = query.getRow(row).get(column);
                    String entity = uri.substring(uri.lastIndexOf("/") + 1).replace("_", " ");
                    SearchResponse<JsonData> search = client.search(s -> s
                            .index("bm25")
                            .query(q -> q
                                    .match(t -> t
                                            .field("content")
                                            .query(entity))), JsonData.class);

                    for (Hit<JsonData> hit : search.hits().hits())
                    {
                        String table = hit.id();
                        double score = hit.score();
                        results.add(new Pair<>(table, score));
                    }
                }
            }

            this.elapsedNs = System.nanoTime() - start;
            return new Result(results.size(), results);
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
