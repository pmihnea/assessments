import java.util.HashSet;

public class GroupHolder {
    public HashSet<Integer> group;

    public GroupHolder(HashSet<Integer> group) {
        this.group = group;
    }

    public GroupHolder() {
        this(new HashSet<>());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupHolder that = (GroupHolder) o;

        return group == that.group;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(group);
    }
}
