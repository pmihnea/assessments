package dboperators.streamjoinoperator;

import dboperators.Row;
import dboperators.joinoperator.SampleColumns;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SampleRelations {

    public static final long MAX_RELATION_SIZE = 1L << 20L;

    public static StreamRelation getRelationXY() {
        final Stream<Row> rows = LongStream.range(0L, MAX_RELATION_SIZE)
                .mapToObj(i -> Stream.of(
                        Row.of("x" + i, "y" + i),
                        // use an offset to not be ordered the stream by X
                        Row.of("x" + (i + MAX_RELATION_SIZE / 3) % MAX_RELATION_SIZE, "y" + i + 1),
                        Row.of("x" + (i + MAX_RELATION_SIZE / 5) % MAX_RELATION_SIZE, "y" + i + 2)
                )).flatMap(Function.identity());
        final StreamRelation r1 = StreamRelation.of(rows, SampleColumns.X, SampleColumns.Y);
        r1.setEstimatedRowsCount(Optional.of(MAX_RELATION_SIZE * 3));
        return r1;
    }

    public static StreamRelation getRelationXZ() {
        final Stream<Row> rows = LongStream.range(0L, MAX_RELATION_SIZE).filter(value -> value % 3 == 0)
                .mapToObj(i -> Stream.of(
                        Row.of("x" + i, "z" + i),
                        // use an offset to not be part of the output join
                        Row.of("x" + i + MAX_RELATION_SIZE, "z" + i),
                        Row.of("x" + i + 2 * MAX_RELATION_SIZE, "z" + i)
                        )).flatMap(Function.identity());
        final StreamRelation r2 = StreamRelation.of(rows, SampleColumns.X, SampleColumns.Z);
        r2.setEstimatedRowsCount(Optional.of((MAX_RELATION_SIZE + 2) / 3 * 3));
        return r2;
    }
}
