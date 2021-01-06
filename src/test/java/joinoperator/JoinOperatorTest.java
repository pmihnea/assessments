package joinoperator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JoinOperatorTest {
    @Test
    public void testXYZ() {
        JoinOperator joinOperator = new JoinOperator();

        final Relation r1 = SampleRelations.getRelation1();
        System.out.println("r1     = " + r1);

        final Relation r2 = SampleRelations.getRelation2();
        System.out.println("r2     = " + r2);

        final Relation output = joinOperator.join(r1, r2);
        System.out.println("output = " + output);

        Assertions.assertArrayEquals(new Column[]{SampleColumns.X, SampleColumns.Y, SampleColumns.Z},
                output.getColumns().toArray(new Column[]{}));

        Assertions.assertArrayEquals(
                new Row[]{
                        Row.of("x1", "y1", "z1"),
                        Row.of("x1", "y2", "z1"),
                        Row.of("x2", "y1", "z2"),
                        Row.of("x2", "y2", "z2")
                },
                output.getRows().toArray(new Row[]{})
        );
    }
}
