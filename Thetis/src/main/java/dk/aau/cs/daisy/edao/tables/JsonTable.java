package dk.aau.cs.daisy.edao.tables;

import java.util.List;
import java.util.Objects;

public class JsonTable {

    public String _id;
    public int numCols;
    public int numDataRows;
    public String pgTitle;
    public int pgId;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableCell tableCell = (TableCell) o;
            return isNumeric == tableCell.isNumeric &&
                    Objects.equals(text, tableCell.text) &&
                    Objects.equals(links, tableCell.links);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text, isNumeric, links);
        }
    }


}
