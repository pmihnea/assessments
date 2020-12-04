import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Counts the groups of connected elements by processing in parallel the relations list, without doing a DFS or BFS.
 */
public class Groups2Par {
    /**
     * related[i] = "010010101..." - means that the elem "i" is related with all elements "j" that
     * represent an index of the character '1' in the given string value
     */

    public enum Strategy {V1,V2}

    public Groups2Par(Strategy strategy) {
        this.strategy = strategy;
    }

    private Strategy strategy;

    public static class Node {
        public int nodeId;
        public String nodeRelations;

        public Node(int nodeId, String rootRelations) {
            this.nodeId = nodeId;
            this.nodeRelations = rootRelations;
        }
    }

    public int countNodeGroups(Stream<Node> relatedStream) {
        switch (strategy){
            case V1: return countNodeGroups_v1(relatedStream);
            case V2: return countNodeGroups_v2(relatedStream);
            default: throw new IllegalStateException("Invalid strategy");
        }
    }

    public int countNodeGroups_v1(Stream<Node> relatedStream) {
        final HashMap<Integer, HashSet<Integer>> item2Group = relatedStream
                .map(this::node2Set)
                .collect(
                        getSupplier_v1(),
                        getAccumulator_v1(),
                        getCombiner_v1()
                );
        long nGroups = item2Group.values().stream().mapToInt(gw -> System.identityHashCode(gw)).distinct().count();
        return (int) nGroups;
    }

    public int countNodeGroups_v2(Stream<Node> relatedStream) {
        final LinkedList<HashSet<Integer>> groups = relatedStream
                .map(this::node2Set).reduce(
                        getIdentity_v2(),
                        getAccumulator_v2(),
                        getCombiner_v2()
                );
        return groups.size();
    }

    private HashSet<Integer> node2Set(Node node) {
        final HashSet<Integer> group = new HashSet<>();
        for (int relation = 0; relation < node.nodeRelations.length(); relation++) {
            if (node.nodeRelations.charAt(relation) == '1') {
                group.add(relation);
            }
        }
        return group;
    }

    private LinkedList<HashSet<Integer>> getIdentity_v2() {
        return new LinkedList<>();
    }

    private BinaryOperator<LinkedList<HashSet<Integer>>> getCombiner_v2() {
        return (groups1, groups2) -> {
            final BiFunction<LinkedList<HashSet<Integer>>, HashSet<Integer>, LinkedList<HashSet<Integer>>> accumulator = getAccumulator_v2();
            LinkedList<HashSet<Integer>> result = new LinkedList<>(groups1);
            for (HashSet<Integer> group2 : groups2) {
                result = accumulator.apply(result, group2);
            }
            return result;
        };
    }

    private BiFunction<LinkedList<HashSet<Integer>>, HashSet<Integer>, LinkedList<HashSet<Integer>>> getAccumulator_v2() {
        return (groups, group) -> {
            final LinkedList<HashSet<Integer>> result = new LinkedList<>();
            HashSet<Integer> mergedGroup = null;
            for (HashSet<Integer> oldGroup : groups) {
                if (oldGroup.stream().filter(elem -> group.contains(elem)).findAny().isPresent()) {
                    if (mergedGroup == null) {
                        mergedGroup = new HashSet<>();
                    }
                    mergedGroup.addAll(oldGroup);
                } else {
                    result.add(oldGroup);
                }
            }
            if (mergedGroup == null || mergedGroup.isEmpty()) {
                result.add(group);
            } else {
                mergedGroup.addAll(group);
                result.add(mergedGroup);
            }
            return result;
        };
    }

    private Supplier<HashMap<Integer, HashSet<Integer>>> getSupplier_v1() {
        return () -> new HashMap<>();
    }

    private BiConsumer<HashMap<Integer, HashSet<Integer>>, HashSet<Integer>> getAccumulator_v1() {
        return (item2group, group) -> {
            //System.out.println("Groups2Par.getAccumulator_v1");
            HashSet<Integer> mergedGroup = new HashSet<>(group);// could we reuse an existing set?
            for (Integer relation : group) {
                HashSet<Integer> relationGroup = item2group.get(relation);
                item2group.put(relation, mergedGroup);
                if (relationGroup != null && relationGroup != mergedGroup /*object identity*/) {
                    // merge the groups into one
                    mergedGroup.addAll(relationGroup);
                    // all added elements need to have the same group
                    for (Integer item : relationGroup) {
                        item2group.put(item, mergedGroup);
                    }
                }
            }
        };
    }


    private BiConsumer<HashMap<Integer, HashSet<Integer>>, HashMap<Integer, HashSet<Integer>>> getCombiner_v1() {
        return (item2group1, item2group2) -> {
            //System.out.println("Groups2Par.getCombiner_v1");
            final BiConsumer<HashMap<Integer, HashSet<Integer>>, HashSet<Integer>> accumulator = getAccumulator_v1();
            final HashSet<Integer> groupsMerged = new HashSet<>(item2group2.size());
            item2group2.values().forEach(group -> {
                if (!groupsMerged.contains(System.identityHashCode(group))) {
                    accumulator.accept(item2group1, group);
                    groupsMerged.add(System.identityHashCode(group));
                }
            });
        };
    }

    public static void main(String[] args) {
        runTests(false, Strategy.V1);
        runTests(true, Strategy.V1);
        runTests(false, Strategy.V2);
        runTests(true, Strategy.V2);
    }

    private static void runTests(boolean parallel, Strategy strategy) {

        test1(parallel, strategy);
        test2(parallel, strategy);
        test3(parallel, strategy);
        test4(parallel, strategy);
        test5(parallel, strategy);
        test6(parallel, strategy);
        test7(parallel, strategy);
        test8(10000, parallel, strategy);
        //test8(100000, parallel, strategy);
        System.out.println();
    }

    private static void assertTest(String test, int expected, IntSupplier actualSupplier, boolean parallel) {
        long start = System.currentTimeMillis();
        int actual = actualSupplier.getAsInt();
        long end = System.currentTimeMillis();
        System.out.println(test + " parallel=" + parallel + " is " + (expected == actual ? "OK " : "NOK")
                + " # expected = " + expected + " # actual = " + actual
                + " # duration [ms] = " + (end - start));
    }

    private static Stream<Node> toNodeStream(List<String> related, boolean parallel) {
        AtomicInteger rootWrapper = new AtomicInteger(-1);
        final List<Node> nodes = related.stream().map(s -> new Node(rootWrapper.addAndGet(1), s))
                .collect(Collectors.toList());
        return parallel ? nodes.parallelStream() : nodes.stream();
    }

    private static void test1(boolean parallel, Strategy strategy) {
        List<String> related = List.of(
                "1"
        );
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test1", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test2(boolean parallel, Strategy strategy) {
        List<String> related = List.of(
                "10",
                "01"
        );
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test2", 2, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test3(boolean parallel, Strategy strategy) {
        List<String> related = List.of(
                "100",
                "010",
                "001"
        );
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test3", 3, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test4(boolean parallel, Strategy strategy) {
        List<String> related = List.of(
                "110",
                "010",
                "001"
        );
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test4", 2, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test5(boolean parallel, Strategy strategy) {
        List<String> related = List.of(
                "111",
                "010",
                "001"
        );
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test5", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test6(boolean parallel, Strategy strategy) {
        List<String> related = List.of(
                "1110",
                "0101",
                "0010",
                "0011"
        );
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test6", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test7(boolean parallel, Strategy strategy) {
        List<String> related = List.of(
                "10100",
                "01010",
                "00100",
                "00010",
                "10011"
        );
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test7", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    /*
    Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 685
    Test8-100000 is OK  # expected = 2 # actual = 2 # duration [ms] = 74534
    all using 390MB
     */
    private static void test8(int N, boolean parallel, Strategy strategy) {
        Stream<Node> related = Stream.generate(new Supplier<Node>() {
            AtomicInteger anInt = new AtomicInteger(-1);

            @Override
            public Node get() {
                int row = anInt.getAndAdd(1);
                char[] value = new char[N];
                row++;
                for (int j = 0; j < N; j++) {
                    value[j] = (row - j) % 10 == 0 ? '1' : '0';
                }
                return new Node(row, new String(value));
            }
        }).limit(N);
        Groups2Par fn = new Groups2Par(strategy);
        assertTest("Test8-" + N, 10,
                () -> fn.countNodeGroups(parallel ? related.parallel() : related), parallel);
    }
}
