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
 * Counts the groups of connected elements by processing sequentially the relations list, without doing a DFS or BFS.
 * It creates more groups and when needed it joins those groups together.
 * Finally it counts how many distinct groups were left. <br>
 * Runtime complexity: <br>
 * Time complexity: O(n^2) - it touches each element, and for each element all its relations: n*n <br>
 * Memory complexity: O(n) - for each element it stores a group wrapper object that stores a set of elements,
 * the sets are in the end the final groups, and each element belongs to only one set at a time. <br>
 * Compared with the DFS/BFS based solution, this one favors the memory complexity over the time complexity.
 * A test with 10e4 and 10e5 elements runs in approx 0.7[s] and respectively 75[s] using 390MB of memory,
 * but a DFS/BFS based solution runs the same tests with 10e4 elements in 0.8[s],
 * but the one with 10e5 elements requires 10GB for the boolean matrix or 1.3GB for a BitSet based matrix,
 * and it it runs in 430[s], respectively 300[s].
 * <p>
 * Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 685
 * Test8-100000 is OK  # expected = 2 # actual = 2 # duration [ms] = 74534
 */
public class Groups2Par {
    /**
     * related[i] = "010010101..." - means that the elem "i" is related with all elements "j" that
     * represent an index of the character '1' in the given string value
     */

    public static class Node {
        public int nodeId;
        public String nodeRelations;

        public Node(int nodeId, String rootRelations) {
            this.nodeId = nodeId;
            this.nodeRelations = rootRelations;
        }
    }

    public int countNodeGroups_v1(Stream<Node> relatedStream) {
        final HashMap<Integer, GroupHolder> item2Group = relatedStream.collect(
                getSupplier_v1(),
                getAccumulator_v1(),
                getCombiner_v1()
        );
        long nGroups = item2Group.values().stream().mapToInt(gw -> System.identityHashCode(gw.group)).distinct().count();
        return (int) nGroups;
    }

    public int countNodeGroups(Stream<Node> relatedStream) {
        final LinkedList<HashSet<Integer>> groups = relatedStream
                .map(node -> {
                    final HashSet<Integer> group = new HashSet<>();
                    for (int relation = 0; relation < node.nodeRelations.length(); relation++) {
                        if (node.nodeRelations.charAt(relation) == '1') {
                            group.add(relation);
                        }
                    }
                    return group;
                }).reduce(
                        getIdentity(),
                        getAccumulator(),
                        getCombiner()
                );
        return groups.size();
    }

    private LinkedList<HashSet<Integer>> getIdentity() {
        return new LinkedList<>();
    }

    private BinaryOperator<LinkedList<HashSet<Integer>>> getCombiner() {
        return (groups1, groups2) -> {
            final BiFunction<LinkedList<HashSet<Integer>>, HashSet<Integer>, LinkedList<HashSet<Integer>>> accumulator = getAccumulator();
            LinkedList<HashSet<Integer>> result = new LinkedList<>(groups1);
            for (HashSet<Integer> group2 : groups2) {
                result = accumulator.apply(result, group2);
            }
            return result;
        };
    }

    private BiFunction<LinkedList<HashSet<Integer>>, HashSet<Integer>, LinkedList<HashSet<Integer>>> getAccumulator() {
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

    private Supplier<HashMap<Integer, GroupHolder>> getSupplier_v1() {
        return () -> new HashMap<>();
    }

    private BiConsumer<HashMap<Integer, GroupHolder>, Node> getAccumulator_v1() {
        return (item2group, node) -> {
            int root = node.nodeId;
            String rootRelations = node.nodeRelations;

            //initialize root group if it doesn't exist yet
            GroupHolder rootGroup = item2group.get(root);
            if (rootGroup == null) {
                rootGroup = new GroupHolder();
                rootGroup.group.add(root);
                item2group.put(root, rootGroup);
            }

            //root relations need to have the same group as root
            for (int relation = 0; relation < rootRelations.length(); relation++) {
                if (rootRelations.charAt(relation) == '1') {
                    GroupHolder relationGroup = item2group.get(relation);
                    if (relationGroup == null) {
                        rootGroup.group.add(relation);
                        relationGroup = new GroupHolder(rootGroup.group);
                        item2group.put(relation, relationGroup);
                    } else if (relationGroup.group != rootGroup.group /*object identity*/) {
                        // merge the groups into one
                        rootGroup.group.addAll(relationGroup.group);
                        // all added elements need to have the same group
                        for (Integer item : relationGroup.group) {
                            GroupHolder itemGroup = item2group.get(item);
                            if (itemGroup == null) {
                                itemGroup = new GroupHolder(rootGroup.group);
                                item2group.put(item, itemGroup);
                            } else {
                                itemGroup.group = rootGroup.group;
                            }
                        }
                    }
                }
            }
        };
    }


    private BiConsumer<HashMap<Integer, GroupHolder>, HashMap<Integer, GroupHolder>> getCombiner_v1() {
        return (item2group1, item2group2) -> {
            for (Integer nodeId : item2group2.keySet()) {
                final GroupHolder relationsGroup1 = item2group1.get(nodeId);
                final GroupHolder relationsGroup2 = item2group2.get(nodeId);
                if (relationsGroup1 == null) {
                    // the node does not exist in the map-1 so it can be copied over from map-2 to map-1
                    item2group1.put(nodeId, relationsGroup2);
                    // the node relations will be copied at their turn
                } else if (relationsGroup1.group != relationsGroup2.group /*object identity*/) {
                    // the node exists in both maps with different groups
                    // merge the two groups into one
                    // all the new elements need to have the same group
                    for (Integer item : relationsGroup2.group) {
                        if (relationsGroup1.group.add(item)) {
                            GroupHolder itemGroup = item2group1.get(item);
                            if (itemGroup == null) {
                                itemGroup = new GroupHolder(relationsGroup1.group);
                            } else {
                                relationsGroup1.group.addAll(itemGroup.group);
                                itemGroup.group = relationsGroup1.group;
                            }
                            item2group1.put(item, itemGroup);
                        }
                    }
                }
            }
        };
    }

    public static void main(String[] args) {
        //runTests(false);
        runTests(true);
    }

    private static void runTests(boolean parallel) {

        test1(parallel);
        test2(parallel);
        test3(parallel);
        test4(parallel);
        test5(parallel);
        test6(parallel);
        test7(parallel);
        test8(10000, parallel);
        test8(100000, parallel);
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

    private static void test1(boolean parallel) {
        List<String> related = List.of(
                "1"
        );
        Groups2Par fn = new Groups2Par();
        assertTest("Test1", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test2(boolean parallel) {
        List<String> related = List.of(
                "10",
                "01"
        );
        Groups2Par fn = new Groups2Par();
        assertTest("Test2", 2, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test3(boolean parallel) {
        List<String> related = List.of(
                "100",
                "010",
                "001"
        );
        Groups2Par fn = new Groups2Par();
        assertTest("Test3", 3, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test4(boolean parallel) {
        List<String> related = List.of(
                "110",
                "010",
                "001"
        );
        Groups2Par fn = new Groups2Par();
        assertTest("Test4", 2, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test5(boolean parallel) {
        List<String> related = List.of(
                "111",
                "010",
                "001"
        );
        Groups2Par fn = new Groups2Par();
        assertTest("Test5", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test6(boolean parallel) {
        List<String> related = List.of(
                "1110",
                "0101",
                "0010",
                "0011"
        );
        Groups2Par fn = new Groups2Par();
        assertTest("Test6", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    private static void test7(boolean parallel) {
        List<String> related = List.of(
                "10100",
                "01010",
                "00100",
                "00010",
                "10011"
        );
        Groups2Par fn = new Groups2Par();
        assertTest("Test7", 1, () -> fn.countNodeGroups(toNodeStream(related, parallel)), parallel);
    }

    /*
    Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 685
    Test8-100000 is OK  # expected = 2 # actual = 2 # duration [ms] = 74534
    all using 390MB
     */
    private static void test8(int N, boolean parallel) {
        Stream<Node> related = Stream.generate(new Supplier<Node>() {
            AtomicInteger anInt = new AtomicInteger(-1);

            @Override
            public Node get() {
                int row = anInt.getAndAdd(1);
                char[] value = new char[N];
                row++;
                for (int j = 0; j < N; j++) {
                    value[j] = (row + j) % 2 == 0 ? '1' : '0';
                }
                return new Node(row, new String(value));
            }
        }).limit(N);
        Groups2Par fn = new Groups2Par();
        assertTest("Test8-" + N, 2,
                () -> fn.countNodeGroups(parallel ? related.parallel() : related), parallel);
    }
}
