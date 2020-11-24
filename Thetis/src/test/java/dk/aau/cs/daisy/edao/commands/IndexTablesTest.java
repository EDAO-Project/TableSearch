package dk.aau.cs.daisy.edao.commands;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import junit.framework.TestCase;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;

public class IndexTablesTest extends TestCase {

    public void testParseTable() {

        JsonTable table = new JsonTable();

        table._id = "test1";
        table.numCols =3;
        table.numDataRows = 4;
        table.pgId = 4;
        table.pgTitle = "Test title";
        table.numNumericCols = 0;
        // table.tableCaption = "Test Caption"; // test with null

        table.header = new ArrayList<>();

        for(int i =0; i<table.numCols; i++){
            table.header.add(new JsonTable.TableCell("head"+i, false, Collections.EMPTY_LIST));
        }

        table.body = new ArrayList<>();

        for (int j = 0; j < table.numDataRows; j++){
            ArrayList<JsonTable.TableCell> row = new ArrayList<>();
            table.body.add(row);
            for(int i =0; i<table.numCols; i++){
                row.add(new JsonTable.TableCell("cell"+i+"_"+j, false, Collections.EMPTY_LIST));
            }
        }

        Gson encoder = new Gson();

        //String jsonString = encoder.toJson(table);

        //System.out.println(jsonString);
        try {
            encoder.toJson(table, new FileWriter("/tmp/test.json"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        JsonTable decoded = null;
        TypeAdapter<JsonTable> strictGsonObjectAdapter =
                new Gson().getAdapter(JsonTable.class);
        try (JsonReader reader = new JsonReader(new FileReader(new File("/tmp/test.json")))) {
            decoded = strictGsonObjectAdapter.read(reader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();

        }



        assertEquals(table._id , decoded._id);
        assertEquals(decoded.header.size(), table.numCols);
        assertEquals(table.header, decoded.header);
        assertEquals(decoded.body.size(), table.numDataRows);
        assertEquals(table.body, decoded.body);

        new File("/tmp/test.json").delete();


    }
}