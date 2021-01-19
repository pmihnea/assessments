package dboperators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;

public class Columns {
    public static ArrayList<Column> of(Column... columns) {
        return new ArrayList<>(Arrays.asList(columns));
    }

    public static ArrayList<Column> union(ArrayList<Column> columns1, ArrayList<Column> columns2) {
        LinkedHashSet<Column> outputColumns = new LinkedHashSet<>();
        outputColumns.addAll(columns1);
        outputColumns.addAll(columns2);
        final ArrayList<Column> outputRelationColumns = new ArrayList<>(outputColumns);
        return outputRelationColumns;
    }

    public static ArrayList<Column> intersection(ArrayList<Column> columns1, ArrayList<Column> columns2) {
        final ArrayList<Column> output = new ArrayList<>();
        for (Column c1 : columns1) {
            if (columns2.contains(c1)) {
                output.add(c1);
            }
        }
        return output;
    }
}
