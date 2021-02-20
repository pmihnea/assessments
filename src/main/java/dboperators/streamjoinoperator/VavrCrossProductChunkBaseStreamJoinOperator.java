package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.Columns;
import dboperators.joinoperator.Index;
import dboperators.joinoperator.JoinOperator;
import dboperators.joinoperator.Relation;
import io.vavr.collection.Iterator;
import io.vavr.collection.Stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Based on vavr library and its cross product function with two streams.
 */
public class VavrCrossProductChunkBaseStreamJoinOperator implements IStreamJoinOperator {
    private static final int CHUNK_SIZE = 1 << 10;

    @Override
    public StreamRelation join(StreamRelation rel1, StreamRelation rel2) {
        Objects.requireNonNull(rel1);
        Objects.requireNonNull(rel2);

        final ArrayList<Column> commonColumns = Columns.intersection(rel1.getColumns(), rel2.getColumns());

        if (commonColumns.size() > 0) {
            // create the columns of the output relation
            ArrayList<Column> outRelColumns = Columns.union(rel1.getColumns(), rel2.getColumns());

            // create the stream of the output relation
            final Iterator<Relation> relations1 = Stream.ofAll(rel1.getRows()).grouped(CHUNK_SIZE)
                    .map(rows -> new Relation(rel1.getColumns(), rows.collect(Collectors.toList())));

            final Iterator<Relation> relations2 = Stream.ofAll(rel2.getRows()).grouped(CHUNK_SIZE)
                    .map(rows -> new Relation(rel2.getColumns(), rows.collect(Collectors.toList())));

            final Iterator<Index> indexes2 = relations2.map(rc2 -> new Index(rc2, commonColumns));

            final Iterator<Relation> outRelations = Stream.ofAll(relations1).crossProduct(Stream.ofAll(indexes2))
                    .map(ri -> JoinOperator.join(ri._1(), ri._2()));

            // create the output relation
            return new StreamRelation(outRelColumns, outRelations.toJavaStream().map(outRel -> outRel.getRows()).flatMap(Collection::stream));
        } else {
            // create a Cartesian product as there is no common column
            //TODO: implement it or leave it as an error
            throw new IllegalStateException("The two input relations have no common column!");
        }
    }
}
