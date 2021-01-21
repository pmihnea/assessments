package dboperators.joinoperator;

import dboperators.Column;
import dboperators.RelationMetadata;
import dboperators.Relations;
import dboperators.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class Relation extends RelationMetadata {
    private List<Row> rows;

    public static Relation of(Column... columns) {
        return new Relation(new ArrayList<>(Arrays.asList(columns)));
    }

    public Relation(ArrayList<Column> columns) {
        super(columns);
        this.rows = new ArrayList<>();
    }

    public Relation(ArrayList<Column> columns, List<Row> rows) {
        super(columns);
        this.rows = rows;
    }

    public List<Row> getRows() {
        return rows;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Relation.class.getSimpleName() + "[", "]")
                .add("columns=" + columns)
                .add("rows=" + rows)
                .toString();
    }

    public Row addMergedRow(Relation rel1, Row row1, Relation rel2, Row row2) {
        Relations.mergeRows(rel1, row1, rel2, row2, this);
        final Row mergedRow = Relations.mergeRows(rel1, row1, rel2, row2, this);
        getRows().add(mergedRow);
        return mergedRow;
    }
}
