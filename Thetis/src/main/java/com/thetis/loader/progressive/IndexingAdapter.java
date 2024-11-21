package com.thetis.loader.progressive;

import com.thetis.structures.Pair;

import java.util.List;
import java.util.Map;

public interface IndexingAdapter
{
    List<Pair<String, Double>> newPriorities(Map<String, Double> currentPriorities);
}
