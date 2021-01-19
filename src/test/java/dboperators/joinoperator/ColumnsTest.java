package dboperators.joinoperator;

import dboperators.Column;
import dboperators.Columns;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class ColumnsTest {
    @Test
    public void testUnion(){
        final ArrayList<Column> union = Columns.union(
                Columns.of(SampleColumns.X,SampleColumns.Y),
                Columns.of(SampleColumns.X, SampleColumns.Z)
        );
        Assertions.assertEquals(Columns.of(SampleColumns.X,SampleColumns.Y, SampleColumns.Z), union);
    }
    @Test
    public void testIntersection(){
        final ArrayList<Column> intersection = Columns.intersection(
                Columns.of(SampleColumns.X,SampleColumns.Y),
                Columns.of(SampleColumns.X, SampleColumns.Z)
        );
        Assertions.assertEquals(Columns.of(SampleColumns.X), intersection);
    }
}
