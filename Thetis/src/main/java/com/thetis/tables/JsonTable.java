package com.thetis.tables;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class JsonTable {

    public String _id;
    public int pgId;

    public int numCols;
    public int numDataRows;
    public int numNumericCols;

    public String pgTitle;
    public String tableCaption;

    public List<TableCell> headers;
    public List<List<TableCell>> rows;

    public JsonTable(){

    }

    public JsonTable(String _id, int numCols, int numDataRows, int numNumericCols, String pgTitle, int pgId, String tableCaption, List<TableCell> header, List<List<TableCell>> rows) {
        this._id = _id;
        this.pgId = pgId;

        this.numCols = numCols;
        this.numDataRows = numDataRows;
        this.numNumericCols = numNumericCols;

        this.pgTitle = pgTitle;
        this.tableCaption = tableCaption;

        this.headers = header;
        this.rows = rows;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public void setPgId(int pgId) {
        this.pgId = pgId;
    }

    public void setNumCols(int numCols) {
        this.numCols = numCols;
    }

    public void setNumDataRows(int numDataRows) {
        this.numDataRows = numDataRows;
    }

    public void setNumNumericCols(int numNumericCols) {
        this.numNumericCols = numNumericCols;
    }

    public void setPgTitle(String pgTitle) {
        this.pgTitle = pgTitle;
    }

    public void setTableCaption(String tableCaption) {
        this.tableCaption = tableCaption;
    }


    public void setHeader(List<TableCell> header) {
        this.headers = header;
    }

    public void setBody(List<List<TableCell>> body) {
        this.rows = rows;
    }

    public void forEach(Consumer<TableCell> consumer) {
        for (List<JsonTable.TableCell> tableRow : this.rows) {
            int columnSize = tableRow.size();

            for (int tableColumn = 0; tableColumn < columnSize; tableColumn++) {
                consumer.accept(tableRow.get(tableColumn));
            }
        }
    }

    public static class TableCell {

        public TableCell(){}

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
