package dk.aau.cs.daisy.edao.store.lsh;

import com.thetis.store.lsh.Distance;
import com.thetis.store.lsh.HammingDistance;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class HammingDistanceTest
{
    @Test
    public void distance0()
    {
        List<Boolean> vec = List.of(true, true, true);
        Distance<Boolean> distance = new HammingDistance<>(vec, vec);
        assertEquals(0, (int) distance.distance());
    }

    @Test
    public void distance1()
    {
        List<Integer> vec1 = List.of(1, 2, 3), vec2 = List.of(1, 2, 1);
        Distance<Integer> distance = new HammingDistance<>(vec1, vec2);
        assertEquals(1, (int) distance.distance());
    }

    @Test
    public void distance2()
    {
        List<Boolean> vec1 = List.of(true, true, false, false), vec2 = List.of(false, true, false, true);
        Distance<Boolean> distance = new HammingDistance<>(vec1, vec2);
        assertEquals(2, (int) distance.distance());
    }
}
