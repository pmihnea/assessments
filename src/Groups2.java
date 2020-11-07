
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Set;

/**
 * Counts the groups of connected elements by processing sequentially the relations list, without doing a a DFS or BFS.
 * It creates more groups and when needed it joins those groups together.
 * Finally it counts how many distinct groups were left in two ways.
 * Runtime complexity:
 *  Time complexity: O(n^2) - it touches each element, and for each element its relations: n*n
 *  Memory complexity: O(n) - for each element it stores a group wrapper object that stores a set of elements,
 *  the sets are in the end the final groups, and each element belongs to only one set at a time.
 */
public class Groups2 {
    private static class GroupWrapper{
        Set<Long> group;

        public GroupWrapper(Set<Long> group) {
            this.group = group;
        }
    }

    public long countGroups(List<String> related) {
        BiMap<Long, GroupWrapper> item2group = HashBiMap.create();
        Long nGroups = Streams.mapWithIndex(related.stream(), (rootRelations, root) -> {
            long newGroups = 0;

            //initialize root group if it doesn't exist yet
            GroupWrapper rootGroup = item2group.get(root);
            if (rootGroup == null) {
                rootGroup = new GroupWrapper(Sets.newHashSet(root));
                item2group.put(root, rootGroup);
                newGroups++;
            }

            //root relations need to have the same group as root
            for (long relation = 0; relation < rootRelations.length(); relation++) {
                if (rootRelations.charAt((int) relation) == '1') {
                    GroupWrapper relationGroup = item2group.get(relation);
                    if (relationGroup == null) {
                        rootGroup.group.add(relation);
                        relationGroup = new GroupWrapper(rootGroup.group);
                        item2group.put(relation, relationGroup);
                    } else if(relationGroup.group != rootGroup.group){
                        rootGroup.group.addAll(relationGroup.group);
                        for (Long item : relationGroup.group) {
                            GroupWrapper itemGroup = item2group.get(item);
                            if(itemGroup == null){
                                itemGroup = new GroupWrapper(rootGroup.group);
                                item2group.put(item, itemGroup);
                            }else{
                                itemGroup.group = rootGroup.group;
                            }
                        }
                        newGroups--;
                    }
                }
            }
            return newGroups;
        }).reduce(0L, Long::sum);

        //count the groups
        long nGroupsV2 = item2group.values().stream().map(gw -> gw.group).distinct().count();
        if(nGroups != nGroupsV2) throw new IllegalStateException("Groups counting failed!");
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
        System.out.println();
    }

    private static void assertTest(String test, long expected, long actual) {
        System.out.println((expected == actual ? "OK " : "NOK")
                + " # expected = " + expected + " # actual = " + actual);
    }

    private static void test1() {
        List<String> related = List.of(
                "1"
        );
        Groups2 fn = new Groups2();
        assertTest("Test1", 1, fn.countGroups(related));
    }

    private static void test2() {
        List<String> related = List.of(
                "10",
                "01"
        );
        Groups2 fn = new Groups2();
        assertTest("Test2", 2, fn.countGroups(related));
    }

    private static void test3() {
        List<String> related = List.of(
                "100",
                "010",
                "001"
        );
        Groups2 fn = new Groups2();
        assertTest("Test3", 3, fn.countGroups(related));
    }

    private static void test4() {
        boolean[][] M = new boolean[][]{
                {true, true, false},
                {false, true, false},
                {false, false, true}
        };
        List<String> related = List.of(
                "110",
                "010",
                "001"
        );
        Groups2 fn = new Groups2();
        assertTest("Test4", 2, fn.countGroups(related));
    }

    private static void test5() {
        boolean[][] M = new boolean[][]{
                {true, true, true},
                {false, true, false},
                {false, false, true}
        };
        List<String> related = List.of(
                "111",
                "010",
                "001"
        );
        Groups2 fn = new Groups2();
        assertTest("Test5", 1, fn.countGroups(related));
    }

    private static void test6() {
        List<String> related = List.of(
                "1110",
                "0101",
                "0010",
                "0011"
        );
        Groups2 fn = new Groups2();
        assertTest("Test6", 1, fn.countGroups(related));
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
        assertTest("Test7", 1, fn.countGroups(related));
    }
}
