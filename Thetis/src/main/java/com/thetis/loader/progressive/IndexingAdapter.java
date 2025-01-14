package com.thetis.loader.progressive;

import com.thetis.structures.Pair;

import java.util.List;

public interface IndexingAdapter
{
    List<Pair<String, Double>> newPriorities();
}
