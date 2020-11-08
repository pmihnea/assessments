import java.util.BitSet;
import java.util.Deque;
import java.util.LinkedList;
import java.util.function.IntSupplier;

/**
 * Counting the groups of connected elements in a not directed graph.
 * n - number of elements
 * Time complexity: O(n) - it visits only once each node
 * Memory complexity: O(n^2) for storing the matrix of relations + additional O(n) for storing the groups -> O(n^2)
 * <p>
 * Using boolean[][] Matrix
 * BFS
 * Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 882
 * Test8-100000 is OK  # expected = 2 # actual = 2 # duration [ms] = 431884 , but it requires approx 10GB of memory to allocate the matrix
 * DFS
 * Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 933
 * Test8-100000 - Exception in thread "main" java.lang.StackOverflowError , expected
 *
 * Using BitSet[] Matrix
 * BFS
 * Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 795
 * Test8-100000 is OK  # expected = 2 # actual = 2 # duration [ms] = 302084 , but it requires 1.3GB of memory to allocate the matrix
 * DFS
 * Test8-10000 is OK  # expected = 2 # actual = 2 # duration [ms] = 720
 * Test8-100000 - Exception in thread "main" java.lang.StackOverflowError , expected
 */
public class Groups1 {
    private BitSet[] M;
    //private boolean[][] M;
    private int n;
    private int[] groups;

    public enum Strategy {DFS, BFS}

    private Strategy strategy;

    public Groups1(BitSet[] M, Strategy strategy) {
        this.n = M.length;
        this.M = M;
        this.strategy = strategy;
        this.groups = new int[n];
    }

    public Groups1(boolean[][] boolM, Strategy strategy) {
        this(new BitSet[boolM.length], strategy);
        // copy the boolean matrix to the bitset array
        for (int i = 0; i < n; i++) {
            this.M[i] = new BitSet(n);
            for (int j = 0; j < n; j++) {
                if (boolM[i][j]) this.M[i].set(j);
            }
        }
    }

    public int countGroups() {
        switch (strategy) {
            case DFS:
                return countGroupsDFS();
            case BFS:
                return countGroupsBFS();
            default:
                throw new IllegalStateException("Invalid strategy");
        }

    }

    public int countGroupsDFS() {
        for (int i = 0; i < n; i++)
            groups[i] = -1;// no group

        // DFS all nodes, if not already visited
        // visiting = assigning a group
        int maxGroup = 0;
        for (int root = 0; root < n; root++) {
            if (!hasGroup(root)) {
                maxGroup++;
                search(root, maxGroup);
            }
        }

        return maxGroup;
    }

    public int countGroupsBFS() {
        for (int i = 0; i < n; i++)
            groups[i] = -1;// no group

        // BFS all nodes, if not already visited
        // visiting = assigning a group
        int maxGroup = 0;
        Deque<Integer> queue = new LinkedList<>();
        for (int root = 0; root < n; root++) {
            if (!hasGroup(root)) {
                maxGroup++;
                setGroup(root, maxGroup);
                queue.addLast(root);
                while (!queue.isEmpty()) {
                    Integer first = queue.removeFirst();
                    for (int j = 0; j < n; j++) {
                        if ((M[first].get(j) || M[j].get(first) || first == j) && !hasGroup(j)) {
                            setGroup(j, maxGroup);
                            queue.addLast(j);
                        }
                    }
                }
            }
        }

        return maxGroup;
    }

    private boolean hasGroup(int root) {
        return groups[root] != -1;
    }

    private void setGroup(int root, int group) {
        groups[root] = group;
    }

    private void search(int root, int group) {
        setGroup(root, group);
        for (int j = 0; j < n; j++) {
            if ((M[root].get(j) || M[j].get(root) || root == j) && !hasGroup(j)) {
                search(j, group);
            }
        }
    }

    public static void main(String[] args) {
        runTests(Strategy.BFS);
        runTests(Strategy.DFS);
    }

    private static void runTests(Strategy strategy) {
        System.out.println("Strategy " + strategy);
        test1(strategy);
        test2(strategy);
        test3(strategy);
        test4(strategy);
        test5(strategy);
        test6(strategy);
        test7(strategy);
        test8(strategy, 10000);
        test8(strategy, 100000);
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

    private static void test1(Strategy strategy) {
        boolean[][] M = new boolean[][]{{true}};
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test1", 1, () -> fn.countGroups());
    }

    private static void test2(Strategy strategy) {
        boolean[][] M = new boolean[][]{
                {true, false},
                {false, true}
        };
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test2", 2, () -> fn.countGroups());
    }

    private static void test3(Strategy strategy) {
        boolean[][] M = new boolean[][]{
                {true, false, false},
                {false, true, false},
                {false, false, true}};
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test3", 3, () -> fn.countGroups());
    }

    private static void test4(Strategy strategy) {
        boolean[][] M = new boolean[][]{
                {true, true, false},
                {false, true, false},
                {false, false, true}
        };
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test4", 2, () -> fn.countGroups());
    }

    private static void test5(Strategy strategy) {
        boolean[][] M = new boolean[][]{
                {true, true, true},
                {false, true, false},
                {false, false, true}
        };
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test5", 1, () -> fn.countGroups());
    }

    private static void test6(Strategy strategy) {
        boolean[][] M = new boolean[][]{
                {true, true, true, false},
                {false, true, false, true},
                {false, false, true, false},
                {false, false, true, true},
        };
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test6", 1, () -> fn.countGroups());
    }

    private static void test7(Strategy strategy) {
        boolean[][] M = new boolean[][]{
                {true, false, true, false, false},
                {false, true, false, true, false},
                {false, false, true, false, false},
                {false, false, false, true, false},
                {true, false, false, true, true}
        };
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test7", 1, () -> fn.countGroups());
    }

    private static void test8(Strategy strategy, int n) {
        BitSet[] M = new BitSet[n];
        for (int i = 0; i < n; i++) {
            M[i] = new BitSet(n);
            for (int j = 0; j < n; j++) {
                if ((i + j) % 2 == 0) M[i].set(j);
            }
        }
        Groups1 fn = new Groups1(M, strategy);
        assertTest("Test8-" + n, 2, () -> fn.countGroups());
    }
}
