package transactionlogs;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProcessTransactionLogs {
    public static void main(String[] args) {
        List<String> logs = List.of(
                "1 2 5",
                "2 3 5",
                "2 3 6",
                "4 4 7",
                "3 6 8"
        );
        System.out.println(processLogs(logs, 2));
    }

    public static List<String> processLogs(List<String> logs, int threshold) {
        //validate input
        if (!(logs != null && logs.size() >= 1 && logs.size() <= 10e5)) {
            return Collections.emptyList();
        }
        // Map to store the mapping from id -> occurrences
        Map<Integer, Long> occurrences =
        // transform the logs into a list of user ids
        logs.stream()
                .flatMap(log -> {
                    String[] items = log.split(" ");
                    if (items[0].equals(items[1])) {
                        return Stream.of(Integer.valueOf(items[0]));
                    } else {
                        return Stream.of(Integer.valueOf(items[0]), Integer.valueOf(items[1]));
                    }
                })
                // count user occurrences
                .collect(Collectors.groupingBy(
                        id -> id,
                        HashMap::new,
                        Collectors.counting()
                ));

        // filter the users ids with occurrences >= threshold
        List<String> result = occurrences.entrySet().stream()
                .filter(entry -> entry.getValue() >= threshold)
                .map(entry -> entry.getKey())
                // sort the users ids
                .sorted()
                // collect the list of users
                .map(Object::toString)
                .collect(Collectors.toList());

        return result;

    }
}
