package joinoperator;

import java.util.ArrayList;
import java.util.Arrays;

public class Relation extends Metadata {
    private ArrayList<Row> rows;

    public static Relation of(Column... columns){
        return new Relation(new ArrayList<>(Arrays.asList(columns)));
    }
    public Relation(ArrayList<Column> columns) {
        super(columns);
        this.rows = new ArrayList<>();
        for(int i=0; i<columns.size(); i++){
            columnIndexMap.put(columns.get(i),i);
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
        Relation outRel = this;
        final Object[] outValues = new Object[outRel.getColumns().size()];
        for (int ci = 0; ci < outRel.getColumns().size(); ci++) {
            final Column outColumn = outRel.getColumns().get(ci);
            int index = rel1.getColumns().indexOf(outColumn);
            if (index >= 0) {
                outValues[ci] = row1.getValues()[index];
            } else {
                index = rel2.getColumns().indexOf(outColumn);
                if (index >= 0) {
                    outValues[ci] = row2.getValues()[index];
                } else {
                    throw new IllegalStateException("An output column cannot be found in any of the two input relations");// TODO: add more error details
                }
            }
        }
        final Row mergedRow = new Row(outValues);
        getRows().add(mergedRow);
        return mergedRow;
    }
}
