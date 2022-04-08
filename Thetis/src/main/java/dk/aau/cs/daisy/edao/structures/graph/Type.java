package dk.aau.cs.daisy.edao.structures.graph;

public class Type
{
    private String type;
    private double idf = -1;

    public Type(String type)
    {
        this.type = type;
    }

    public Type(String type, double idf)
    {
        this(type);
        this.idf = idf;
    }

    @Override
    public String toString()
    {
        return this.type;
    }

    public String getType()
    {
        return toString();
    }

    public double getIdf()
    {
        return this.idf;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Type))
            return false;

        Type other = (Type) o;
        return this.type.equals(other.type) && this.idf == other.idf;
    }
}
