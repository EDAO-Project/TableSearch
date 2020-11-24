package dk.aau.cs.daisy.edao.commands;

import com.google.gson.Gson;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import junit.framework.TestCase;

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
        // table.tableCaption = "Test Caption"; // test with null

        table.header = new ArrayList<>();

        for(int i =0; i<table.numCols; i++){
            table.header.add(new JsonTable.TableCell("head"+i, false, Collections.EMPTY_LIST));
        }

        for (int j = 0; j < table.numDataRows; j++){
            for(int i =0; i<table.numCols; i++){
                table.header.add(new JsonTable.TableCell("cell"+i+"_"+j, false, Collections.EMPTY_LIST));
            }
        }

        Gson encoder = new Gson();

        String jsonString = encoder.toJson(table);
        System.out.println(jsonString);

        JsonTable decoded = encoder.fromJson(jsonString, JsonTable.class);

        assertEquals(table._id , decoded._id);
        assertEquals(table.header, decoded.header);
        assertEquals(decoded.body.size(), 4);
        assertEquals(table.body, decoded.body);


    }
}