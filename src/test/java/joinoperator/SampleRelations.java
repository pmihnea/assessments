package joinoperator;

public class SampleRelations {
    public static Relation getRelationXY(){
        final Relation r1 = Relation.of(SampleColumns.X, SampleColumns.Y);
        r1.getRows().add(Row.of("x1","y1"));
        r1.getRows().add(Row.of("x1","y2"));
        r1.getRows().add(Row.of("x2","y1"));
        r1.getRows().add(Row.of("x2","y2"));
        r1.getRows().add(Row.of("x3","y3"));
        return r1;
    }
    public static Relation getRelationXZ(){
        final Relation r2 = Relation.of(SampleColumns.X, SampleColumns.Z);
        r2.getRows().add(Row.of("x1","z1"));
        r2.getRows().add(Row.of("x2","z2"));
        return r2;
    }
    public static Relation getEmptyRelationXYZ(){
        final Relation r3 = Relation.of(SampleColumns.X,SampleColumns.Y, SampleColumns.Z);
        return r3;
    }

    public static Relation getRelationPXY(){
        final Relation r1 = Relation.of(SampleColumns.P,SampleColumns.X, SampleColumns.Y);
        r1.getRows().add(Row.of("p1","x1","y1"));
        r1.getRows().add(Row.of("p1","x1","y2"));
        r1.getRows().add(Row.of("p2","x2","y1"));
        r1.getRows().add(Row.of("p2","x2","y2"));
        r1.getRows().add(Row.of("p3","x3","y3"));
        r1.getRows().add(Row.of("p4","x3","y4"));
        return r1;
    }

    public static Relation getRelationQXY(){
        final Relation r1 = Relation.of(SampleColumns.Q,SampleColumns.X, SampleColumns.Y);
        r1.getRows().add(Row.of("q1","x1","y1"));
        r1.getRows().add(Row.of("q1","x1","y2"));
        r1.getRows().add(Row.of("q2","x2","y1"));
        r1.getRows().add(Row.of("q2","x2","y2"));
        r1.getRows().add(Row.of("q3","x3","y3"));
        r1.getRows().add(Row.of("q4","x4","y3"));
        return r1;
    }

}
