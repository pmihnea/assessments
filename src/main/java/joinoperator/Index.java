package joinoperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Index extends Metadata {
    private Relation relation;

    private Map<Row, ArrayList<Row>> values;

    public Index(Relation relation, ArrayList<Column> columns) {
        super(columns);
        this.relation = relation;
        this.values = new HashMap<>();
        initValues();
    }

    public Relation getRelation() {
        return relation;
    }

    public Map<Row, ArrayList<Row>> getValues() {
        return values;
    }

    private void initValues() {
        for (Row relationRow : relation.getRows()) {
            final Row indexRow = createIndexRowFromRelationRow(relation, relationRow);
            ArrayList<Row> existingRelationRows = values.computeIfAbsent(indexRow, k -> new ArrayList<>());
            existingRelationRows.add(relationRow);
        }
    }

    public Row createIndexRowFromRelationRow(Relation relation, Row relationRow) {
        // copy the relation values to the index values
        Object[] indexValues = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int ri = Optional.ofNullable(relation.getColumnIndexMap().get(columns.get(i)))
                    .orElseThrow(() -> new IllegalStateException("Index columns cannot be found in the relation set of columns!"));
            indexValues[i] = relationRow.getValues()[ri];
        }
        return new Row(indexValues);
    }
}
