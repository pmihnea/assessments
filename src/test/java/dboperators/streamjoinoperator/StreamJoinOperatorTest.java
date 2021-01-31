package dboperators.streamjoinoperator;

import dboperators.Row;
import dboperators.joinoperator.JoinOperator;
import dboperators.joinoperator.Relation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dboperators.streamjoinoperator.SampleRelations.MAX_RELATION_SIZE;

public class StreamJoinOperatorTest {
    private static Stream<IStreamJoinOperator> streamJoinImpl() {
        return Stream.of(
                new StreamJoinOperator1(),
                new StreamJoinOperator2(),
                new StreamJoinOperator3(),
                new StreamJoinOperator4()
        );
    }

    @ParameterizedTest
    @MethodSource("streamJoinImpl")
    public void testXYZ_StreamJoin(IStreamJoinOperator joinOperator) {
        System.out.println("MAX_RELATION_SIZE = " + MAX_RELATION_SIZE);

        final StreamRelation r1 = SampleRelations.getRelationXY();
        System.out.println("r1     = " + r1);

        final StreamRelation r2 = SampleRelations.getRelationXZ();
        System.out.println("r2     = " + r2);

        final StreamRelation output = joinOperator.join(r2, r1);
        System.out.println("out    = " + output);
        final long outputRowsCount = output.getRows().count();
        //final long outputRowsCount = output.getRows().takeWhile(counter(100)).count();
        System.out.println("output rows count = " + outputRowsCount);

        Assertions.assertEquals((MAX_RELATION_SIZE + 2) / 3 * 3, outputRowsCount);
        //Assertions.assertEquals(100, outputRowsCount);
    }

    private Predicate<Row> counter(final int max) {
        return new Predicate<Row>() {

            int count = 0;

            @Override
            public boolean test(Row row) {
                count++;
                return count <= max;
            }
        };
    }

    @Test
    public void testXYZ_Join() {
        final StreamRelation r1 = SampleRelations.getRelationXY();
        Relation rf1 = new Relation(r1.getColumns(), r1.getRows().collect(Collectors.toList()));
        System.out.println("rf1 columns = " + rf1.getColumns());
        System.out.println("rf1 rows count = " + rf1.getRows().size());

        final StreamRelation r2 = SampleRelations.getRelationXZ();
        Relation rf2 = new Relation(r2.getColumns(), r2.getRows().collect(Collectors.toList()));
        System.out.println("rf2 columns = " + rf2.getColumns());
        System.out.println("rf2 rows count = " + rf2.getRows().size());

        final Relation output = JoinOperator.join(rf2, rf1);
        final long outputRowsCount = output.getRows().size();
        System.out.println("output columns = " + output.getColumns());
        System.out.println("output rows count = " + outputRowsCount);

        Assertions.assertEquals((MAX_RELATION_SIZE + 2) / 3 * 3, outputRowsCount);
    }
}
