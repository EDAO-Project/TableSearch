package dk.aau.cs.daisy.edao.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dk.aau.cs.daisy.edao.store.EmbeddingsIndex;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.table.Table;
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

        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build())
        {
            ElasticsearchTransport transport = new RestClientTransport(client, new JacksonJsonpMapper());
            ElasticsearchClient searchClient = new ElasticsearchClient(transport);
            List<Pair<String, Double>> results = new ArrayList<>();

            for (int row = 0; row < queryRows; row++)
            {
                int queryColumns = query.getRow(row).size();

                for (int column = 0; column < queryColumns; column++)
                {
                    String entity = query.getRow(row).get(column);
                    SearchResponse<String> search = searchClient.search(s -> s
                            .index("bm25")
                            .query(q -> q
                                    .term(t -> t
                                            .field("content")
                                            .value(entity))), String.class);

                    for (Hit<String> hit : search.hits().hits())
                    {
                        results.add(new Pair<>(hit.source(), hit.score()));
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
