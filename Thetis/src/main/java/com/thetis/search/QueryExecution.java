package com.thetis.search;

import com.thetis.structures.table.Table;

public class QueryExecution extends Execution<Table<String>, Result>
{
    public QueryExecution(AnalogousSearch search)
    {
        super(search::search);
    }

    @Override
    public Result execute(Table<String> query)
    {
        return super.execution.apply(query);
    }
}
