package com.thetis.store.lsh;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ElementShingles implements Shingles
{
    private Set<List<String>> shingles;

    public static Set<List<String>> shingles(Set<String> elements, int shingleSize)
    {
        return new ElementShingles(elements, shingleSize).shingles();
    }

    public ElementShingles(Set<String> elements, int shingleSize)
    {
        this.shingles = compute(elements, shingleSize);
    }

    private static Set<List<String>> compute(Set<String> elements, int size)
    {
        List<Set<String>> duplicates = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
        {
            duplicates.add(elements);
        }

        Set<List<String>> product = Sets.cartesianProduct(duplicates);
        product = product.stream().filter(t -> {
            String first = t.get(0);

            for (String element : t)
            {
                if (!element.equals(first))
                {
                    return true;
                }
            }

            return false;
        }).collect(Collectors.toSet());

        return product;
    }

    @Override
    public Set<List<String>> shingles()
    {
        return this.shingles;
    }
}
