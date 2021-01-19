package dboperators.streamjoinoperator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamJoinOperatorTest {
    @Test
    public void testXYZ() {
        StreamJoinOperator joinOperator = new StreamJoinOperator();

        final StreamRelation r1 = SampleRelations.getRelationXY();
        System.out.println("r1     = " + r1);

        final StreamRelation r2 = SampleRelations.getRelationXZ();
        System.out.println("r2     = " + r2);

        final StreamRelation output = joinOperator.join(r1, r2);
        System.out.println("out    = " + output);
        final long outputRowsCount = output.getRows().count();
        System.out.println("output rows count = " + outputRowsCount);

        Assertions.assertEquals(SampleRelations.MAX_RELATION_SIZE, outputRowsCount);
    }
}
