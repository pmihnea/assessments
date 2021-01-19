package dboperators;

import java.util.Optional;

public class Relations {
    public static Row mergeRows(RelationMetadata rel1, Row row1, RelationMetadata rel2, Row row2, RelationMetadata outRel) {
        final Object[] outValues = new Object[outRel.getColumns().size()];
        for (int i = 0; i < outRel.getColumns().size(); i++) {
            final Column outColumn = outRel.getColumns().get(i);
            outValues[i] =
                    Optional.ofNullable(rel1.getColumnIndexMap().get(outColumn))
                            .map(ri -> row1.getValues()[ri])
                            .orElseGet(() ->
                                    Optional.ofNullable(rel2.getColumnIndexMap().get(outColumn))
                                            .map(ri -> row2.getValues()[ri])
                                            .orElseThrow(() -> new IllegalStateException("An output column cannot be found in any of the two input relations"))
                            );
        }
        return new Row(outValues);
    }
}
