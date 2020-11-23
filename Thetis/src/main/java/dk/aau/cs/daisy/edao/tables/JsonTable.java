package dk.aau.cs.daisy.edao.tables;

import java.util.List;

public class JsonTable {

   public String _id;
   public int numCols;
   public int numDataRows;
   public String pgTitle;
   public String pgId;
   public String tableCaption;

   public List<TableCell> header;

   public List<List<TableCell>> body;


    public static class TableCell {
        public String text;
        public boolean isNumeric;
        public List<String> links;
    }

}
