package com.thetis.loader.progressive.adapter;

import com.google.gson.Gson;
import com.thetis.search.Result;
import com.thetis.structures.Pair;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.*;

public class GTAdapter implements IndexingAdapter
{
    private final Map<String, Double> gt;
    private final Result result;

    public GTAdapter(Result result, File groundTruth)
    {
        this.gt = readGT(groundTruth);
        this.result = result;
    }

    private static Map<String, Double> readGT(File gtFile)
    {
        try (Reader reader = Files.newBufferedReader(gtFile.toPath()))
        {
            Gson gson = new Gson();
            Type type = new TypeToken<HashMap<String, Double>(){}.getType();

            return gson.fromJson(reader, type);
        }

        catch (IOException e)
        {
            return new HashMap<>();
        }
    }

    @Override
    public List<Pair<String, Double>> newPriorities()
    {
        List<Pair<String, Double>> priorities = new ArrayList<>();
        Iterator<Pair<String, Double>> iter = this.result.getResults();

        while (iter.hasNext())
        {
            Pair<String, Double> pair = iter.next();

            if (this.gt.containsKey(pair.getFirst()) && Math.abs(this.gt.get(pair.getFirst()) - pair.getSecond()) >= 1.00)
            {
                priorities.add(new Pair<>(pair.getFirst(), (double) 10));
            }
        }

        return priorities;
    }
}
