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

    public static Row extractRow(RelationMetadata fromRelation, Row fromRow, RelationMetadata outRelation) {
        // copy the fromRelation values to the output values
        Object[] outValues = new Object[outRelation.getColumns().size()];
        for (int i = 0; i < outRelation.getColumns().size(); i++) {
            int ri = Optional.ofNullable(fromRelation.getColumnIndexMap().get(outRelation.getColumns().get(i)))
                    .orElseThrow(() -> new IllegalStateException("Output columns cannot be found in the fromRelation set of columns!"));
            outValues[i] = fromRow.getValues()[ri];
        }
        return new Row(outValues);
    }
}
