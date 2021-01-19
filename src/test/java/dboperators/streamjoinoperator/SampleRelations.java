package dboperators.streamjoinoperator;

import dboperators.Row;
import dboperators.joinoperator.SampleColumns;
import io.vavr.collection.Stream;

public class SampleRelations {

    public static final int MAX_RELATION_SIZE = 1 << 20;

    public static StreamRelation getRelationXY(){
        final java.util.stream.Stream<Row> rows = Stream.range(0, MAX_RELATION_SIZE)
                .flatMap(i -> Stream.of(Row.of("x" + i, "y" + i), Row.of("x" + i, "y" + i + 1))).toJavaStream();
        final StreamRelation r1 = StreamRelation.of(rows, SampleColumns.X, SampleColumns.Y);
        return r1;
    }
    public static StreamRelation getRelationXZ(){
        final java.util.stream.Stream<Row> rows = Stream.rangeBy(0, MAX_RELATION_SIZE,2)
                .flatMap(i -> Stream.of(Row.of("x"+i,"z"+i))).toJavaStream();
        final StreamRelation r2 = StreamRelation.of(rows, SampleColumns.X, SampleColumns.Z);
        return r2;
    }
}
