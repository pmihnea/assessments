package joinoperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Index {
    private Relation relation;
    private ArrayList<Column> columns;
    private Map<Row, ArrayList<Row>> values;

    public Index(Relation relation, ArrayList<Column> columns) {
        this.relation = relation;
        this.columns = columns;
        this.values = new HashMap<>();
        initValues();
    }

    public Relation getRelation() {
        return relation;
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public Map<Row, ArrayList<Row>> getValues() {
        return values;
    }

    private void initValues() {
        for (Row relationRow : relation.getRows()) {
            final Row indexRow = createIndexRowFromRelationRow(relation, relationRow);

            ArrayList<Row> existingRelationRows = values.get(indexRow);
            if (existingRelationRows == null) {
                existingRelationRows = new ArrayList<>();
                values.put(indexRow, existingRelationRows);
            }
            existingRelationRows.add(relationRow);
        }
    }

    public Row createIndexRowFromRelationRow(Relation relation, Row relationRow) {
        // copy the index values
        Object[] indexValues = new Object[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            final int i = relation.getColumns().indexOf(columns.get(c));
            if(i < 0) throw new IllegalStateException("Index columns cannot be found in relation set of columns!");
            indexValues[c] = relationRow.getValues()[i];
        }
        return new Row(indexValues);
    }
}
