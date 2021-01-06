package joinoperator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
