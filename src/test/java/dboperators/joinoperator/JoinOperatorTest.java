package dboperators.joinoperator;

import dboperators.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JoinOperatorTest {
    private <T> Set<T> asSet(T... elems) {
        final HashSet<T> set = new HashSet<>(elems.length);
        set.addAll(Arrays.asList(elems));
        return set;
    }

    private <T> Set<T> asSet(List<T> elems) {
        final HashSet<T> set = new HashSet<>(elems.size());
        set.addAll(elems);
        return set;
    }

    @Test
    public void testXYZ() {
        final Relation r1 = SampleRelations.getRelationXY();
        System.out.println("r1     = " + r1);

        final Relation r2 = SampleRelations.getRelationXZ();
        System.out.println("r2     = " + r2);

        final Relation output = JoinOperator.join(r1, r2);
        System.out.println("output = " + output);

        Assertions.assertEquals(asSet(SampleColumns.X, SampleColumns.Y, SampleColumns.Z),
                asSet(output.getColumns()));

        Assertions.assertEquals(
                asSet(
                        Row.of("x1", "y1", "z1"),
                        Row.of("x1", "y2", "z1"),
                        Row.of("x2", "y1", "z2"),
                        Row.of("x2", "y2", "z2")
                ),
                asSet(output.getRows())
        );
    }

    @Test
    public void testPXYQ() {
        final Relation r1 = SampleRelations.getRelationPXY();
        System.out.println("r1     = " + r1);

        final Relation r2 = SampleRelations.getRelationQXY();
        System.out.println("r2     = " + r2);

        final Relation output = JoinOperator.join(r1, r2);
        System.out.println("output = " + output);

        Assertions.assertEquals(asSet(SampleColumns.P, SampleColumns.X, SampleColumns.Y, SampleColumns.Q),
                asSet(output.getColumns()));
        Assertions.assertEquals(
                asSet(
                        Row.of("p1", "x1", "y1", "q1"),
                        Row.of("p1", "x1", "y2", "q1"),
                        Row.of("p2", "x2", "y1", "q2"),
                        Row.of("p2", "x2", "y2", "q2"),
                        Row.of("p3", "x3", "y3", "q3")
                ),
                asSet(output.getRows())
        );
    }
}
