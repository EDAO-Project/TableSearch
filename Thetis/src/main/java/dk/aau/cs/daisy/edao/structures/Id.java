package dk.aau.cs.daisy.edao.structures;

import dk.aau.cs.daisy.edao.system.Configuration;

import java.io.Serializable;

public class Id implements Serializable
{
    private static class IdAllocator
    {
        private static int allocatedId = -1;

        public Id allocId()
        {
            if (allocatedId == -1)
            {
                String id = Configuration.getLargestId();

                if (id == null)
                    allocatedId = 0;

                else
                    allocatedId = Integer.parseInt(id) + 1;
            }

            Configuration.setLargestId(String.valueOf(allocatedId));
            return Id.copy(allocatedId++);
        }
    }

    private int id;

    public static Id copy(int id)
    {
        return new Id(id);
    }

    /**
     * Only run-time unique
     * @return New run-time unique identifier
     */
    public static Id alloc()
    {
        return new IdAllocator().allocId();
    }

    public Id(int id)
    {
        this.id = id;
        IdAllocator.allocatedId = id + 1;
    }

    public int getId()
    {
        return this.id;
    }

    @Override
    public int hashCode()
    {
        return this.id;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Id))
            return false;

        Id otherId = (Id) other;
        return this.id == otherId.id;
    }
}
