package com.thetis.structures;

import com.thetis.system.Configuration;

import java.io.Serializable;

/**
 * ID of entities
 * IDs are allocated in incrementing order
 * The largest ID is stored in a configuration file so no entities are given the same ID
 */
public class Id implements Serializable, Comparable<Id>
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

    /**
     * Represents a global ID
     * @return Global ID
     */
    public static Id any()
    {
        return new Id(-1);
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

    @Override
    public int compareTo(Id other)
    {
        if (this.id == other.getId())
            return 0;

        return this.id < other.getId() ? -1 : 1;
    }

    @Override
    public String toString()
    {
        return "|" + this.id + "|";
    }
}
