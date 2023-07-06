package com.thetis.loader;

import com.thetis.connector.Neo4jEndpoint;

import java.util.List;

public class WikiLinker implements Linker
{
    private Neo4jEndpoint neo4j;

    public WikiLinker(Neo4jEndpoint neo4j)
    {
        this.neo4j = neo4j;
    }

    @Override
    public String link(String mention) throws IllegalArgumentException
    {
        if (!mention.contains("wikipedia.org"))
        {
            throw new IllegalArgumentException("Mention is not a Wikipedia link");
        }

        List<String> links = this.neo4j.searchLink(mention);
        return !links.isEmpty() ? links.get(0) : null;
    }
}
