package joinoperator;

public class Relations {
    public static Row mergeRows(Relation rel1, Row row1, Relation rel2, Row row2, Relation outRel) {
        final Object[] outValues = new Object[outRel.getColumns().size()];
        for (int ci = 0; ci < outRel.getColumns().size(); ci++) {
            final Column outColumn = outRel.getColumns().get(ci);
            int index = rel1.getColumns().indexOf(outColumn);
            if (index >= 0) {
                outValues[ci] = row1.getValues()[index];
            } else {
                index = rel2.getColumns().indexOf(outColumn);
                if (index >= 0) {
                    outValues[ci] = row2.getValues()[index];
                } else {
                    throw new IllegalStateException("An output column cannot be found in any of the two input relations");// TODO: add more error details
                }
            }
        }
        return new Row(outValues);
    }
}
