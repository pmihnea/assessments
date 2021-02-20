package dboperators.joinoperator;

import dboperators.Column;
import dboperators.RelationMetadata;
import dboperators.Relations;
import dboperators.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class Index extends RelationMetadata {
    private Relation relation;

    private Map<Row, ArrayList<Row>> values;

    public Index(Relation relation, ArrayList<Column> columns) {
        super(columns);
        this.relation = relation;
        this.values = new HashMap<>();
        addIndexRows();
    }

    public Relation getRelation() {
        return relation;
    }

    public Map<Row, ArrayList<Row>> getValues() {
        return values;
    }

    private void addIndexRows() {
        for (Row relationRow : relation.getRows()) {
            addIndexRow(relationRow);
        }
    }

    public ArrayList<Row> addIndexRow(Row relationRow) {
        final Row indexRow = Relations.extractRow(relation, relationRow, this);
        ArrayList<Row> existingRelationRows = values.computeIfAbsent(indexRow, k -> new ArrayList<>());
        existingRelationRows.add(relationRow);
        return existingRelationRows;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Index.class.getSimpleName() + "[", "]")
                .add("columns=" + columns)
                .add("values=" + values)
                .toString();
    }
}
