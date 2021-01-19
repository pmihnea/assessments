package dboperators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RelationMetadata {
    protected ArrayList<Column> columns;
    protected Map<Column, Integer> columnIndexMap;

    public RelationMetadata(ArrayList<Column> columns) {
        this.columns = columns;
        this.columnIndexMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnIndexMap.put(columns.get(i), i);
        }
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public Map<Column, Integer> getColumnIndexMap() {
        return columnIndexMap;
    }
}
