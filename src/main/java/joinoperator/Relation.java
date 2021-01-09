package joinoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

public class Relation extends Metadata {
    private ArrayList<Row> rows;

    public static Relation of(Column... columns) {
        return new Relation(new ArrayList<>(Arrays.asList(columns)));
    }

    public Relation(ArrayList<Column> columns) {
        super(columns);
        this.rows = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            columnIndexMap.put(columns.get(i), i);
        }
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
        final Object[] outValues = new Object[getColumns().size()];
        for (int i = 0; i < getColumns().size(); i++) {
            final Column outColumn = getColumns().get(i);
            outValues[i] =
                    Optional.ofNullable(rel1.getColumnIndexMap().get(outColumn))
                            .map(ri -> row1.getValues()[ri])
                            .orElseGet(() ->
                                    Optional.ofNullable(rel2.getColumnIndexMap().get(outColumn))
                                            .map(ri -> row2.getValues()[ri])
                                            .orElseThrow(() -> new IllegalStateException("An output column cannot be found in any of the two input relations"))
                            );
        }
        final Row mergedRow = new Row(outValues);
        getRows().add(mergedRow);
        return mergedRow;
    }
}
