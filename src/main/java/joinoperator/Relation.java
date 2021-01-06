package joinoperator;

import java.util.ArrayList;
import java.util.Arrays;

public class Relation {
    private ArrayList<Column> columns;
    private ArrayList<Row> rows;

    public static Relation of(Column... columns){
        return new Relation(new ArrayList<>(Arrays.asList(columns)));
    }
    public Relation(ArrayList<Column> columns) {
        this.columns = columns;
        this.rows = new ArrayList<>();
    }

    public ArrayList<Column> getColumns() {
        return columns;
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
}
