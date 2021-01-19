package dboperators.joinoperator;

import dboperators.Column;
import dboperators.RelationMetadata;
import dboperators.Relations;
import dboperators.Row;

import java.util.ArrayList;
import java.util.Arrays;

public class Relation extends RelationMetadata {
    private ArrayList<Row> rows;

    public static Relation of(Column... columns) {
        return new Relation(new ArrayList<>(Arrays.asList(columns)));
    }

    public Relation(ArrayList<Column> columns) {
        super(columns);
        this.rows = new ArrayList<>();
    }

    public ArrayList<Row> getRows() {
        return rows;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Relation{");
        sb.append("columns=").append(columns);
        sb.append(", rows=").append(rows);
        sb.append('}');
        return sb.toString();
    }

    public Row addMergedRow(Relation rel1, Row row1, Relation rel2, Row row2) {
        Relations.mergeRows(rel1, row1, rel2, row2, this);
        final Row mergedRow = Relations.mergeRows(rel1, row1, rel2, row2, this);
        getRows().add(mergedRow);
        return mergedRow;
    }
}
