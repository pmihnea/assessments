package dboperators.joinoperator;

import dboperators.Column;
import dboperators.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class IndexTest {
    @Test
    public void testOneColumnIndexCreation() {
        final ArrayList<Column> indexColumns = new ArrayList<>();
        indexColumns.add(SampleColumns.X);

        final Index index = new Index(SampleRelations.getRelationXY(), indexColumns);

        final Map<Row, ArrayList<Row>> values = index.getValues();
        Assertions.assertEquals(
                Set.of(
                        Row.of("x1"),
                        Row.of("x2"),
                        Row.of("x3")
                ),
                values.keySet()
        );

        assertX1Rows(values);
        assertX2Rows(values);
        assertX3Rows(values);
    }

    private void assertX1Rows(Map<Row, ArrayList<Row>> values) {
        final ArrayList<Row> x1Rows = new ArrayList<>();
        x1Rows.add(Row.of("x1","y1"));
        x1Rows.add(Row.of("x1","y2"));
        Assertions.assertEquals(x1Rows, values.get(Row.of("x1")));
    }
    private void assertX2Rows(Map<Row, ArrayList<Row>> values) {
        final ArrayList<Row> x1Rows = new ArrayList<>();
        x1Rows.add(Row.of("x2","y1"));
        x1Rows.add(Row.of("x2","y2"));
        Assertions.assertEquals(x1Rows, values.get(Row.of("x2")));
    }
    private void assertX3Rows(Map<Row, ArrayList<Row>> values) {
        final ArrayList<Row> x1Rows = new ArrayList<>();
        x1Rows.add(Row.of("x3","y3"));
        Assertions.assertEquals(x1Rows, values.get(Row.of("x3")));
    }
}
