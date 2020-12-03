import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
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
public class Groups2 {

    /**
     * related[i] = "010010101..." - means that the elem "i" is related with all elements "j" that
     * represent an index of the character '1' in the given string value
     */
    public int countGroups(List<String> related) {
        return countGroups(related.stream());
    }

    public int countGroups(Stream<String> relatedStream) {
        Map<Integer, GroupHolder> item2group = new HashMap<>();
        AtomicInteger rootWrapper = new AtomicInteger(-1);
        Integer nGroups = relatedStream.map(rootRelations -> {
            int root = rootWrapper.addAndGet(1);
            int newGroups = 0;

            //initialize root group if it doesn't exist yet
            GroupHolder rootGroup = item2group.get(root);
            if (rootGroup == null) {
                rootGroup = new GroupHolder();
                rootGroup.group.add(root);
                item2group.put(root, rootGroup);
                newGroups++; // a new group was created
            }

            //root relations need to have the same group as root
            for (int relation = 0; relation < rootRelations.length(); relation++) {
                if (rootRelations.charAt(relation) == '1') {
                    GroupHolder relationGroup = item2group.get(relation);
                    if (relationGroup == null) {
                        rootGroup.group.add(relation);
                        relationGroup = new GroupHolder(rootGroup.group);
                        item2group.put(relation, relationGroup);
                    } else if (relationGroup.group != rootGroup.group) {
                        // merge the groups into one
                        rootGroup.group.addAll(relationGroup.group);
                        for (Integer item : relationGroup.group) {
                            GroupHolder itemGroup = item2group.get(item);
                            if (itemGroup == null) {
                                itemGroup = new GroupHolder(rootGroup.group);
                                item2group.put(item, itemGroup);
                            } else {
                                itemGroup.group = rootGroup.group;
                            }
                        }
                        newGroups--; // two groups were merged into one
                    }
                }
            }
            return newGroups;
        }).reduce(0, Integer::sum);

        //count the groups
        //int nGroupsV2 = item2group.values().stream().map(gw -> gw.group).distinct().count();
        //if(nGroups != nGroupsV2) throw new IllegalStateException("Groups counting failed!");
        return nGroups;
    }

    public static void main(String[] args) {
        runTests();
    }

    private static void runTests() {
        test1();
        test2();
        test3();
        test4();
        test5();
        test6();
        test7();
        test8(10000);
        test8(100000);
        System.out.println();
    }

    private static void assertTest(String test, int expected, IntSupplier actualSupplier) {
        long start = System.currentTimeMillis();
        int actual = actualSupplier.getAsInt();
        long end = System.currentTimeMillis();
        System.out.println(test + " is " + (expected == actual ? "OK " : "NOK")
                + " # expected = " + expected + " # actual = " + actual
                + " # duration [ms] = " + (end - start));
    }

    private static void test1() {
        List<String> related = List.of(
                "1"
        );
        Groups2 fn = new Groups2();
        assertTest("Test1", 1, () -> fn.countGroups(related));
    }

    private static void test2() {
        List<String> related = List.of(
                "10",
                "01"
        );
        Groups2 fn = new Groups2();
        assertTest("Test2", 2, () -> fn.countGroups(related));
    }

    private static void test3() {
        List<String> related = List.of(
                "100",
                "010",
                "001"
        );
        Groups2 fn = new Groups2();
        assertTest("Test3", 3, () -> fn.countGroups(related));
    }

    private static void test4() {
        List<String> related = List.of(
                "110",
                "010",
                "001"
        );
        Groups2 fn = new Groups2();
        assertTest("Test4", 2, () -> fn.countGroups(related));
    }

    private static void test5() {
        List<String> related = List.of(
                "111",
                "010",
                "001"
        );
        Groups2 fn = new Groups2();
        assertTest("Test5", 1, () -> fn.countGroups(related));
    }

    private static void test6() {
        List<String> related = List.of(
                "1110",
                "0101",
                "0010",
                "0011"
        );
        Groups2 fn = new Groups2();
        assertTest("Test6", 1, () -> fn.countGroups(related));
    }

    private static void test7() {
        List<String> related = List.of(
                "10100",
                "01010",
                "00100",
                "00010",
                "10011"
        );
        Groups2 fn = new Groups2();
        assertTest("Test7", 1, () -> fn.countGroups(related));
    }

    /*
    Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 685
    Test8-100000 is OK  # expected = 2 # actual = 2 # duration [ms] = 74534
    all using 390MB
     */
    private static void test8(int N) {
        Stream<String> related = Stream.generate(new Supplier<String>() {
            int row = -1;
            char[] value = new char[N];

            @Override
            public String get() {
                row++;
                for (int j = 0; j < N; j++) {
                    value[j] = (row + j) % 2 == 0 ? '1' : '0';
                }
                return new String(value);
            }
        }).limit(N);
        Groups2 fn = new Groups2();
        assertTest("Test8-" + N, 2, () -> fn.countGroups(related));
    }
}
