package dboperators.streamjoinoperator;

import dboperators.Row;
import dboperators.joinoperator.SampleColumns;
import io.vavr.collection.Stream;

import java.util.Optional;

public class SampleRelations {

    public static final long MAX_RELATION_SIZE = 1 << 17;

    public static StreamRelation getRelationXY() {
        final java.util.stream.Stream<Row> rows = Stream.range(0, MAX_RELATION_SIZE)
                .flatMap(i -> Stream.of(
                        Row.of("x" + i, "y" + i),
                        // use an offset to not be ordered the stream by X
                        Row.of("x" + (i + MAX_RELATION_SIZE / 3) % MAX_RELATION_SIZE, "y" + i + 1),
                        Row.of("x" + (i + MAX_RELATION_SIZE / 5) % MAX_RELATION_SIZE, "y" + i + 2)
                )).toJavaStream();
        final StreamRelation r1 = StreamRelation.of(rows, SampleColumns.X, SampleColumns.Y);
        r1.setEstimatedRowsCount(Optional.of(MAX_RELATION_SIZE * 3));
        return r1;
    }

    public static StreamRelation getRelationXZ() {
        final java.util.stream.Stream<Row> rows = Stream.rangeBy(MAX_RELATION_SIZE - 1, -1, -3)
                .flatMap(i -> Stream.of(
                        Row.of("x" + i, "z" + i),
                        // use an offset to not be part of the output join
                        Row.of("x" + i + MAX_RELATION_SIZE, "z" + i),
                        Row.of("x" + i + 2 * MAX_RELATION_SIZE, "z" + i)
                        )).toJavaStream();
        final StreamRelation r2 = StreamRelation.of(rows, SampleColumns.X, SampleColumns.Z);
        r2.setEstimatedRowsCount(Optional.of((MAX_RELATION_SIZE + 2) / 3 * 3));
        return r2;
    }
}
