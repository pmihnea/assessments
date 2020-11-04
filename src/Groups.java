import java.util.Deque;
import java.util.LinkedList;

/**
 * Counting the groups of connected elements in a not directed graph.
 * n - number of elements
 * Time complexity: O(n) - it visits only once each node
 * Memory complexity: O(n^2) for storing the matrix of relations + additional O(n) for storing the groups
 */
public class Groups {
    private boolean[][] M;
    private int n;
    private int[] groups;
    public enum Strategy{DFS,BFS}
    private Strategy strategy;

    public Groups(boolean[][] M, Strategy strategy) {
        this.M = M;
        n = M.length;
        groups = new int[n];
        this.strategy = strategy;
    }
    public int countGroups() {
        switch (strategy){
            case BFS: return countGroupsDFS();
            case DFS: return countGroupsBFS();
            default: throw new IllegalStateException("Invalid strategy");
        }

    }
    public int countGroupsDFS() {
        for (int i = 0; i < n; i++)
            groups[i] = -1;// no group

        // DFS all nodes, if not already visited
        // visiting = assign a group
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
        // visiting = assign a group
        int maxGroup = 0;
        Deque<Integer> queue = new LinkedList<>();
        for (int root = 0; root < n; root++) {
            if (!hasGroup(root)) {
                maxGroup++;
                setGroup(root, maxGroup);
                queue.addLast(root);
                while(!queue.isEmpty()){
                    Integer first = queue.removeFirst();
                    for(int j=0; j<n; j++){
                        if ((M[first][j] || M[j][first] || first == j) && !hasGroup(j)) {
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
            if ((M[root][j] || M[j][root] || root == j) && !hasGroup(j)) {
                search(j, group);
            }
        }
    }

    public static void main(String[] args) {
        runTests(Strategy.DFS);
        runTests(Strategy.BFS);
    }

    private static void runTests(Strategy strategy) {
        System.out.println("Strategy "+strategy);
        test1(strategy);
        test2(strategy);
        test3(strategy);
        test4(strategy);
        test5(strategy);
        test6(strategy);
        System.out.println();
    }

    private static void assertTest(String test, int expected, int actual){
        System.out.println((expected == actual ? "OK " : "NOK")
                + " # expected = "+ expected + " # actual = " + actual);
    }
    private static void test1(Strategy strategy) {
        boolean[][] M = new boolean[][]{{true}};
        Groups fn = new Groups(M, strategy);
        assertTest("Test1", 1,fn.countGroups());
    }

    private static void test2(Strategy strategy) {
        boolean[][] M = new boolean[][]{{true, false}, {false, true}};
        Groups fn = new Groups(M, strategy);
        assertTest("Test2", 2,fn.countGroups());
    }

    private static void test3(Strategy strategy) {
        boolean[][] M = new boolean[][]{{true, false, false}, {false, true, false}, {false, false, true}};
        Groups fn = new Groups(M, strategy);
        assertTest("Test3", 3,fn.countGroups());
    }

    private static void test4(Strategy strategy) {
        boolean[][] M = new boolean[][]{
            {true, true, false},
            {false, true, false},
            {false, false, true}
        };
        Groups fn = new Groups(M, strategy);
        assertTest("Test4", 2,fn.countGroups());
    }

    private static void test5(Strategy strategy) {
        boolean[][] M = new boolean[][]{
            {true, true, true},
            {false, true, false},
            {false, false, true}
        };
        Groups fn = new Groups(M, strategy);
        assertTest("Test5", 1,fn.countGroups());
    }
    private static void test6(Strategy strategy) {
        boolean[][] M = new boolean[][]{
            {true, true, true, false},
            {false, true, false, true},
            {false, false, true, false},
            {false, false, true, true},
        };
        Groups fn = new Groups(M, strategy);
        assertTest("Test6", 1,fn.countGroups());
    }
}
