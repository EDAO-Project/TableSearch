package dk.aau.cs.daisy.edao.commands;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.thetis.tables.JsonTable;
import junit.framework.TestCase;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class IndexTablesTest extends TestCase {

    public void testParseTable() {

        JsonTable table = new JsonTable();

        table._id = "test1";
        table.numCols =3;
        table.numDataRows = 4;
        table.pgId = 13856394;
        table.pgTitle = "Test title";
        table.numNumericCols = 0;
        // table.tableCaption = "Test Caption"; // test with null

        table.headers = new ArrayList<>();

        for(int i =0; i<table.numCols; i++){
            table.headers.add(new JsonTable.TableCell("head"+i, false, Collections.EMPTY_LIST));
        }

        table.rows = new ArrayList<>();

        for (int j = 0; j < table.numDataRows; j++){
            ArrayList<JsonTable.TableCell> row = new ArrayList<>();
            table.rows.add(row);
            for(int i =0; i<table.numCols; i++){
                row.add(new JsonTable.TableCell("cell"+i+"_"+j, false, Arrays.asList("http://www.wikipedia.org/wiki/Gerd_Langholen"+i)));
            }
        }

        Gson encoder = new Gson();

        String jsonString = encoder.toJson(table);

        //System.out.println("String serialized as:\n"+jsonString);

        encoder = new Gson();
        try (FileWriter writer = new FileWriter("/tmp/test.json")) {
            encoder.toJson(table, writer);
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
        assertEquals(decoded.headers.size(), table.numCols);
        assertEquals(table.headers, decoded.headers);
        assertEquals(decoded.rows.size(), table.numDataRows);
        assertEquals(table.rows, decoded.rows);

        new File("/tmp/test.json").delete();


    }
}