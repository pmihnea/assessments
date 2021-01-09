package joinoperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Metadata {
    protected ArrayList<Column> columns;
    protected Map<Column, Integer> columnIndexMap;

    public Metadata(ArrayList<Column> columns) {
        this.columns = columns;
        this.columnIndexMap = new HashMap<>();
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public Map<Column, Integer> getColumnIndexMap() {
        return columnIndexMap;
    }
}
