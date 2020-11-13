# TableSearch

## Table Search Algorithm based on Semantic Relevance

Semantically Augmented Table Search


### Outline

1- Input: Set of tables & and a KG

2- Preprocessing: 
    
    Take tables and output an index that has <tableId, rowId, cellId, uriToEntity>

3- On-Line:
    
    Take input a set of entity tuples:
        <Entity1, Entity2>
        <Entity3, Entity4>

    Return a set of ranked tables
        T1, T2, T3 --> ranked based on relevance score
