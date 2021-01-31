package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.Columns;
import dboperators.joinoperator.Index;
import dboperators.joinoperator.JoinOperator;
import dboperators.joinoperator.Relation;
import io.vavr.Function1;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Iterator;
import io.vavr.collection.List;
import io.vavr.collection.Stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
It uses the vavr library for sequential stream/iterator operations.<br>
Implements a custom join operation based on the following steps:
 <ul>
<li>first groups both relations rows into a stream of chunks of rows of fixed size,</li>
<li>then maps one stream of chunked relations into indexes </li>
<li>then  creates a cross product between the stream of chunked relations and the stream of chunked indexes by
 <ul>
  <li>zipping the two streams into pairs of current list of relations and current list of indexes </li>
 <li>and then joining the index head with the relation head, the index head with all tail elements of the relations list,
  the relation head with all tail elements of the indexes list </li>
 <li>and then concatenating all those above result streams </li>
 </ul>
</li>
 </ul>
 */
public class StreamJoinOperator4 implements IStreamJoinOperator {
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
            final Iterator<List<Relation>> relations1 = Stream.ofAll(rel1.getRows()).grouped(CHUNK_SIZE)
                    .map(rows -> new Relation(rel1.getColumns(), rows.collect(Collectors.toList())))
                    .map(toIntermediaryList());

            final Iterator<Relation> relations2 = Stream.ofAll(rel2.getRows()).grouped(CHUNK_SIZE)
                    .map(rows -> new Relation(rel2.getColumns(), rows.collect(Collectors.toList())));
            final Iterator<List<Index>> indexes2 = relations2.map(rc2 -> new Index(rc2, commonColumns))
                    .map(toIntermediaryList());

            final JoinOperator joinOperator = new JoinOperator();
            final Iterator<Relation> outRelations = this.zipAll(relations1, indexes2, toLastWhenNull(), toLastWhenNull())
                    .flatMap(relationIndexLists -> {
                        final List<Relation> cRelations1 = relationIndexLists._1();
                        final List<Index> cIndexes2 = relationIndexLists._2();

                        if(cRelations1.head() == null && cIndexes2.head() == null){
                            return Iterator.empty();//this case should not happen, but it is safer to check
                        }

                        if(cRelations1.head() == null){
                            return cRelations1.tail().iterator().map(relation -> JoinOperator.join(relation, cIndexes2.head()));
                        }

                        if(cIndexes2.head() == null){
                            return cIndexes2.tail().iterator().map(index -> JoinOperator.join(cRelations1.head(), index));
                        }

                        // both heads are not null
                        final Iterator<Relation> out1 = cRelations1.iterator().map(relation -> JoinOperator.join(relation, cIndexes2.head()));
                        final Iterator<Relation> out2 = cIndexes2.tail().iterator().map(index -> JoinOperator.join(cRelations1.head(), index));
                        return out1.concat(out2);
                    });


            // create the output relation
            return new StreamRelation(outRelColumns, outRelations.toJavaStream().map(Relation::getRows).flatMap(Collection::stream));
        } else {
            // create a Cartesian product as there is no common column
            //TODO: implement it or leave it as an error
            throw new IllegalStateException("The two input relations have no common column!");
        }
    }

    private <T> Function<List<T>, List<T>> toLastWhenNull() {
        return new Function1<>() {
            List<T> last = List.empty();
            List<T> lastWhenNull = null;

            @Override
            public List<T> apply(List<T> relations) {
                if (relations != null) {
                    last = relations;
                    return relations;
                } else {
                    if(lastWhenNull == null) {
                        lastWhenNull = last.prepend(null);
                    }
                    return lastWhenNull;
                }
            }
        };
    }

    private <T> Function<T, List<T>> toIntermediaryList() {
        return new Function<>() {
            List<T> acc = List.empty();

            @Override
            public List<T> apply(T relation) {
                return acc = acc.prepend(relation);
            }
        };
    }

    private  <T,U> Iterator<Tuple2<T, U>> zipAll(Iterable<? extends T> thisIterable,
                                                 Iterable<? extends U> thatIterable,
                                                 Function<? super T,? extends T> thisElem,
                                                 Function<? super U, ? extends U> thatElem) {
        Objects.requireNonNull(thisIterable, "this iterable is null");
        final java.util.Iterator<? extends T> thisIt = thisIterable.iterator();
        Objects.requireNonNull(thatIterable, "that iterable is null");
        final java.util.Iterator<? extends U> thatIt = thatIterable.iterator();

        if (!thisIt.hasNext() && !thatIt.hasNext()) {
            return Iterator.empty();
        } else {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return thisIt.hasNext() || thatIt.hasNext();
                }

                @Override
                public Tuple2<T, U> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    final T v1 = thisIt.hasNext() ? thisElem.apply(thisIt.next()) : thisElem.apply(null);
                    final U v2 = thatIt.hasNext() ? thatElem.apply(thatIt.next()) : thatElem.apply(null);
                    return Tuple.of(v1, v2);
                }
            };
        }
    }
}
