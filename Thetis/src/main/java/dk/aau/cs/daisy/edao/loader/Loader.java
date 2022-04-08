package dk.aau.cs.daisy.edao.loader;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dk.aau.cs.daisy.edao.commands.parser.TableParser;
import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.graph.Entity;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.graph.Type;
import dk.aau.cs.daisy.edao.system.Configuration;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.utils;
import org.apache.commons.collections.IteratorUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;

public class Loader
{
    private List<Path> files;
    private File outputPath;
    private int threads, loadedTables = 0, cellsWithLinks = 0;
    private long elapsed = -1;
    private Map<Integer, Integer> cellToNumLinksFrequency = new HashMap<>();
    private Map<Integer, Integer> linkToNumEntitiesFrequency = new HashMap<>();
    private Neo4jEndpoint neo4j;
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;
    private BloomFilter<String> filter = BloomFilter.create(
            Funnels.stringFunnel(Charset.defaultCharset()),
            5_000_000,
            0.01);
    private final Map<String, Stats> tableStats = new TreeMap<>();

    private static final List<String> DISALLOWED_ENTITY_TYPES =
            Arrays.asList("http://www.w3.org/2002/07/owl#Thing", "http://www.wikidata.org/entity/Q5");

    public Loader(List<Path> files, File outputDir, Neo4jEndpoint neo4j, int threads)
    {
        if (!outputDir.exists())
            outputDir.mkdirs();

        else if (!outputDir.isDirectory())
            throw new IllegalArgumentException("Output directory '" + outputDir + "' is not a directory");

        this.files = files;
        this.outputPath = outputDir;
        this.neo4j = neo4j;
        this.threads = threads;
        this.linker = new EntityLinking();
        this.entityTable = new EntityTable();
        this.entityTableLink = new EntityTableLink();
    }

    /**
     * Loading of tables
     */
    public void load() throws IOException
    {
        if (this.loadedTables > 0)
            throw new RuntimeException("Loading has already complete");

        int size = files.size();
        long startTime = System.nanoTime();

        // TODO: Maybe parallelize this process
        for (int i = 0; i < size; i++)
        {
            if (load(files.get(i).toAbsolutePath()))
                this.loadedTables++;
        }

        loadIDFs();
        flushToDisk();
        writeStats();
        this.elapsed = System.nanoTime() - startTime;
    }

    private boolean load(Path file)
    {
        JsonTable table = TableParser.parse(file);

        if (table == null || table._id  == null || table.rows == null)
            return false;

        // Maps a cell specified by RowNumber, ColumnNumber to the list of entities it matches to
        Map<Pair<Integer, Integer>, List<String>> entityMatches = new HashMap<>();

        // The set of entities corresponding to this filename/table
        Set<String> setOfEntities = new HashSet<>();
        int rowId = 0;

        for (List<JsonTable.TableCell> row : table.rows)
        {
            int collId =0;

            for (JsonTable.TableCell cell : row)
            {
                if (!cell.links.isEmpty())
                {
                    List<String> matchedUris = new ArrayList<>();
                    this.cellsWithLinks++;
                    this.cellToNumLinksFrequency.merge(cell.links.size(), 1, Integer::sum);

                    for(String link : cell.links)
                    {
                        String uri;

                        if ((uri = this.linker.mapTo(link)) != null)
                            matchedUris.add(uri);

                        else
                        {
                            List<String> tempLinks = this.neo4j.searchLink(link.replace("http://www.", "http://en."));

                            if (!tempLinks.isEmpty())
                            {
                                String entity = tempLinks.get(0);
                                matchedUris.add(entity);
                                this.linker.addMapping(link, entity);
                                this.linkToNumEntitiesFrequency.merge(tempLinks.size(), 1, Integer::sum);

                                List<String> entityTypesUris = this.neo4j.searchTypes(entity);

                                for (String type : DISALLOWED_ENTITY_TYPES)
                                {
                                    entityTypesUris.remove(type);
                                }

                                List<Type> types = new ArrayList<>(entityTypesUris.size());
                                entityTypesUris.forEach(t -> types.add(new Type(t)));
                                this.entityTable.insert(this.linker.getDictionary().get(entity), new Entity(entity, types));
                            }
                        }

                        if (this.linker.mapTo(link) != null)
                        {
                            String entity = this.linker.mapTo(link);
                            Id entityId = this.linker.getDictionary().get(entity);
                            List<dk.aau.cs.daisy.edao.structures.Pair<Integer, Integer>> tableLocation =
                                    List.of(new dk.aau.cs.daisy.edao.structures.Pair<>(rowId, collId));
                            String fileName = file.getFileName().toString();
                            this.entityTableLink.addLocation(entityId, fileName, tableLocation);
                        }
                    }

                    if (!matchedUris.isEmpty())
                    {
                        for (String entity : matchedUris)
                        {
                            this.filter.put(entity);
                            setOfEntities.add(entity);
                        }

                        entityMatches.put(new Pair<>(rowId, collId), matchedUris);
                    }
                }

                collId++;
            }

            rowId++;
        }

        saveStats(table, file.getFileName().toString(),
                Set.copyOf(IteratorUtils.toList(this.linker.getDictionary().keys().asIterator())), entityMatches);
        return true;
    }

    private void saveStats(JsonTable jTable, String tableFileName, Set<String> entities, Map<Pair<Integer, Integer>, List<String>> entityMatches)
    {
        Stats stats = collectStats(jTable, tableFileName, entities, entityMatches);
        this.tableStats.put(tableFileName, stats);
    }

    private Stats collectStats(JsonTable jTable, String tableFileName, Set<String> entities, Map<Pair<Integer, Integer>, List<String>> entityMatches)
    {
        List<Integer> numEntitiesPerRow = new ArrayList<>(Collections.nCopies(jTable.numDataRows, 0));
        List<Integer> numEntitiesPerCol = new ArrayList<>(Collections.nCopies(jTable.numCols, 0));
        List<Integer> numCellToEntityMatchesPerCol = new ArrayList<Integer>(Collections.nCopies(jTable.numCols, 0));
        List<Boolean> tableColumnsIsNumeric = new ArrayList<Boolean>(Collections.nCopies(jTable.numCols, false));
        long numCellToEntityMatches = 0L; // Specifies the total number (bag semantics) of entities all cells map to

        for (String entity : entities)
        {
            Id entityId = this.linker.getDictionary().get(entity);

            if (entityId != null)
            {
                List<dk.aau.cs.daisy.edao.structures.Pair<Integer, Integer>> locations = this.entityTableLink.getLocations(entityId, tableFileName);

                for (Pair<Integer, Integer> location : locations)
                {
                    numEntitiesPerRow.set(location.getFirst(), numEntitiesPerRow.get(location.getFirst()) + 1);
                    numEntitiesPerCol.set(location.getSecond(), numEntitiesPerCol.get(location.getSecond()) + 1);
                    numCellToEntityMatches++;
                }
            }
        }

        for (Pair<Integer, Integer> position : entityMatches.keySet())
        {
            Integer colId = position.getSecond();
            numCellToEntityMatchesPerCol.set(colId, numCellToEntityMatchesPerCol.get(colId) + 1);
        }

        if (jTable.numNumericCols == jTable.numCols)
            tableColumnsIsNumeric = new ArrayList<Boolean>(Collections.nCopies(jTable.numCols, true));

        else
        {
            int colId = 0;

            for (JsonTable.TableCell cell : jTable.rows.get(0))
            {
                if (cell.isNumeric)
                    tableColumnsIsNumeric.set(colId, true);

                colId++;
            }
        }

        return Stats.build()
                .rows(jTable.numDataRows)
                .columns(jTable.numCols)
                .cells(jTable.numDataRows * jTable.numCols)
                .entities(entities.size())
                .mappedCells(entityMatches.size())
                .entitiesPerRow(numEntitiesPerRow)
                .entitiesPerColumn(numEntitiesPerCol)
                .cellToEntityMatches(numCellToEntityMatches)
                .entities(entities)
                .cellToEntityMatchesPerCol(numCellToEntityMatchesPerCol)
                .numericTableColumns(tableColumnsIsNumeric)
                .finish();
    }

    private void writeStats()
    {
        try
        {
            FileWriter writer = new FileWriter(this.outputPath + "/" + Configuration.getWikiLinkToEntitiesFrequencyFile());
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(this.linkToNumEntitiesFrequency, writer);
            writer.flush();
            writer.close();

            writer = new FileWriter(this.outputPath + "/" + Configuration.getCellToNumLinksFrequencyFile());
            gson.toJson(this.cellToNumLinksFrequency, writer);
            writer.flush();
            writer.close();

            writer = new FileWriter(this.outputPath + "/" + Configuration.getTableStatsFile());
            gson.toJson(this.tableStats, writer);
            writer.flush();
            writer.close();
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void loadIDFs()
    {
        loadEntityIDFs();
        loadTypeIDFs();
    }

    private void loadEntityIDFs()
    {
        Iterator<String> entityIter = this.linker.getDictionary().keys().asIterator();

        while (entityIter.hasNext())
        {
            String entityStr = entityIter.next();
            Id entityId = this.linker.getDictionary().get(entityStr);
            List<String> entityFiles = this.entityTableLink.find(entityId);
            double idf = Math.log10((double) this.loadedTables / entityFiles.size()) + 1;

            Entity entity = this.entityTable.find(entityId);
            this.entityTable.remove(entityId);
            entity.setIDF(idf);
            this.entityTable.insert(entityId, entity);
        }
    }

    private void loadTypeIDFs()
    {
        Map<Type, Integer> typeFrequency = new HashMap<>();
        Iterator<Id> idIter = this.linker.getDictionary().elements().asIterator();

        while (idIter.hasNext())
        {
            Entity entity = this.entityTable.find(idIter.next());

            if (entity != null)
            {
                List<Type> types = entity.getTypes();

                for (Type type : types)
                {
                    if (typeFrequency.containsKey(type))
                        typeFrequency.put(type, typeFrequency.get(type) + 1);

                    else
                        typeFrequency.put(type, 1);
                }
            }
        }

        for (Type type : typeFrequency.keySet())
        {
            double ratio = (double) this.linker.getDictionary().size() / typeFrequency.get(type);
            double idf = utils.log2(ratio);
            updateTypeIDFs(type.getType(), idf);
        }
    }

    private void updateTypeIDFs(String typeName, double idf)
    {
        Iterator<Id> idsIter = this.linker.getDictionary().elements().asIterator();

        while (idsIter.hasNext())
        {
            Id entityId = idsIter.next();
            Entity entity = this.entityTable.find(entityId);
            List<Type> types = entity.getTypes();
            this.entityTable.remove(entityId);
            types.replaceAll(t -> {
                if (t.getType().equals(typeName))
                    return new Type(typeName, idf);

                return t;
            });
        }
    }

    private void flushToDisk() throws IOException
    {
        // Entity linker
        ObjectOutputStream outputStream =
                new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getEntityLinkerFile()));
        outputStream.writeObject(this.linker);
        outputStream.flush();
        outputStream.close();

        // Entity table
        outputStream = new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getEntityTableFile()));
        outputStream.writeObject(this.entityTable);
        outputStream.flush();
        outputStream.close();

        // Entity to tables inverted index
        outputStream = new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getEntityToTablesFile()));
        outputStream.writeObject(this.entityTableLink);
        outputStream.flush();
        outputStream.close();

        genNeo4jTableMappings();
    }

    private void genNeo4jTableMappings() throws IOException
    {
        FileOutputStream outputStream = new FileOutputStream(this.outputPath + "/" + Configuration.getTableToEntitiesFile());
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        Iterator<String> entityIter = this.linker.getDictionary().keys().asIterator();

        while (entityIter.hasNext())
        {
            String entity = entityIter.next();
            List<String> tables = this.entityTableLink.find(this.linker.getDictionary().get(entity));

            for (String table : tables)
            {
                writer.write("<http://thetis.edao.eu/wikitables/\"" + table +
                        "> <https://schema.org/mentions> <" + entity + "> .\n");
            }
        }

        writer.flush();
        writer.close();
        outputStream = new FileOutputStream(Configuration.getTableToTypesFile());
        writer = new OutputStreamWriter(outputStream);
        Set<String> tables = new HashSet<>();
        Iterator<Id> entityIdIter = this.linker.getDictionary().elements().asIterator();

        while (entityIdIter.hasNext())
        {
            List<String> entityTables = this.entityTableLink.find(entityIdIter.next());

            for (String t : entityTables)
            {
                if (tables.contains(t))
                    continue;

                tables.add(t);
                writer.write("<http://thetis.edao.eu/wikitables/" + t + "> " +
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                        "<https://schema.org/Table> .\n");
            }
        }

        writer.flush();
        writer.close();
    }

    /**
     * Elapsed time of loading
     * @return Elapsed time of loading
     */
    public long elapsedTime()
    {
        return this.elapsed;
    }

    /**
     * Number of successfully loaded tables
     * @return Number of successfully loaded tables
     */
    public int loadedTables()
    {
        return this.loadedTables;
    }

    public int cellsWithLinks()
    {
        return this.cellsWithLinks;
    }

    /**
     * Entity linker getter
     * @return Entity linker from link to entity URI
     */
    public EntityLinking getEntityLinker()
    {
        return this.linker;
    }

    /**
     * Getter to Entity table
     * @return Loaded entity table
     */
    public EntityTable getEntityTable()
    {
        return this.entityTable;
    }

    /**
     * Getter to entity-table linker
     * @return Loaded entity-table linker
     */
    public EntityTableLink getEntityTableLinker()
    {
        return this.entityTableLink;
    }

    public long getApproximateEntityMentions()
    {
        return this.filter.approximateElementCount();
    }

    public Map<String, Stats> getTableStats()
    {
        return this.tableStats;
    }
}
