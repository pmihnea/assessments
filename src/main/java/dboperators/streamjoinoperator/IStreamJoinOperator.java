package dboperators.streamjoinoperator;

public interface IStreamJoinOperator {
    StreamRelation join(StreamRelation rel1, StreamRelation rel2);
}
