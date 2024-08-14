package com.thetis.loader.progressive.recorder;

/**
 * A dummy recorder that always return 1.0 for the alpha variable
 * Use this when no alpha boosting is needed
 */
public class DummyRecorder extends QueryRecorder
{
    @Override
    public double abstractBoost(String id)
    {
        return 1.0;
    }
}
