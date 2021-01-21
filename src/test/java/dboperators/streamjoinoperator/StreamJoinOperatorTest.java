package dboperators.streamjoinoperator;

import dboperators.joinoperator.JoinOperator;
import dboperators.joinoperator.Relation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static dboperators.streamjoinoperator.SampleRelations.MAX_RELATION_SIZE;

public class StreamJoinOperatorTest {
    @Test
    public void testXYZ_StreamJoin() {
        StreamJoinOperator joinOperator = new StreamJoinOperator();

        final StreamRelation r1 = SampleRelations.getRelationXY();
        System.out.println("r1     = " + r1);

        final StreamRelation r2 = SampleRelations.getRelationXZ();
        System.out.println("r2     = " + r2);

        final StreamRelation output = joinOperator.join(r2, r1);
        System.out.println("out    = " + output);
        final long outputRowsCount = output.getRows().count();
        System.out.println("output rows count = " + outputRowsCount);

        Assertions.assertEquals((MAX_RELATION_SIZE + 2) / 3 * 3, outputRowsCount);
    }

    @Test
    public void testXYZ_Join() {
        JoinOperator joinOperator = new JoinOperator();

        final StreamRelation r1 = SampleRelations.getRelationXY();
        Relation rf1 = new Relation(r1.getColumns(), r1.getRows().collect(Collectors.toList()));
        System.out.println("rf1 columns = " + rf1.getColumns());
        System.out.println("rf1 rows count = " + rf1.getRows().size());

        final StreamRelation r2 = SampleRelations.getRelationXZ();
        Relation rf2 = new Relation(r2.getColumns(), r2.getRows().collect(Collectors.toList()));
        System.out.println("rf2 columns = " + rf2.getColumns());
        System.out.println("rf2 rows count = " + rf2.getRows().size());

        final Relation output = joinOperator.join(rf2, rf1);
        final long outputRowsCount = output.getRows().size();
        System.out.println("output columns = " + output.getColumns());
        System.out.println("output rows count = " + outputRowsCount);

        Assertions.assertEquals((MAX_RELATION_SIZE + 2) / 3 * 3, outputRowsCount);
    }
}
