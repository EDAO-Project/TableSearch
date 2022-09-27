package dk.aau.cs.daisy.edao.store.lsh;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TypeShingles implements Shingles
{
    private Set<List<String>> shingles;

    public static Set<List<String>> shingles(Set<String> types, int shingleSize)
    {
        return new TypeShingles(types, shingleSize).shingles();
    }

    public TypeShingles(Set<String> types, int shingleSize)
    {
        this.shingles = compute(types, shingleSize);
    }

    private static Set<List<String>> compute(Set<String> types, int size)
    {
        List<Set<String>> duplicates = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
        {
            duplicates.add(types);
        }

        Set<List<String>> product = Sets.cartesianProduct(duplicates);
        product = product.stream().filter(t -> {
            String first = t.get(0);

            for (String type : t)
            {
                if (!type.equals(first))
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
