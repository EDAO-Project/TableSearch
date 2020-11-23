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

    public JsonTable(){

    }

    public JsonTable(String _id, int numCols, int numDataRows, String pgTitle, String pgId, String tableCaption, List<TableCell> header, List<List<TableCell>> body) {
        this._id = _id;
        this.numCols = numCols;
        this.numDataRows = numDataRows;
        this.pgTitle = pgTitle;
        this.pgId = pgId;
        this.tableCaption = tableCaption;
        this.header = header;
        this.body = body;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public void setNumCols(int numCols) {
        this.numCols = numCols;
    }


    public void setNumDataRows(int numDataRows) {
        this.numDataRows = numDataRows;
    }


    public void setPgId(String pgId) {
        this.pgId = pgId;
    }


    public void setPgTitle(String pgTitle) {
        this.pgTitle = pgTitle;
    }

    public void setTableCaption(String tableCaption) {
        this.tableCaption = tableCaption;
    }


    public void setHeader(List<TableCell> header) {
        this.header = header;
    }

    public void setBody(List<List<TableCell>> body) {
        this.body = body;
    }



    public static class TableCell {

        public TableCell(){};

        public TableCell(String text, boolean isNumeric, List<String> links) {
            this.text = text;
            this.isNumeric = isNumeric;
            this.links = links;
        }

        public String text;
        public boolean isNumeric;
        public List<String> links;
    }


}
