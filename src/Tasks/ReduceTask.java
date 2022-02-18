package Tasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The class for a task that solves only the Reduce operation
 */
public class ReduceTask {
    private final String fileName;
    private final List<Map<Integer, Integer>> mappedResultsList;
    private final List<List<String>> longestWordsList;
    private final Map<Integer, Integer> combinedMappedResults;
    private final List<String> combinedLongestWords;
    private float fileRank;
    private int maxLength;
    private int maxLengthCount;

    /**
     * Recreates the function F that returns Fibonacci values mapped onto the lengths of the words in a given file
     * @param value parameter of the function F
     * @return the Fibonacci value in correspondence with the given value
     */
    public int getFibonacciValue(int value) {
        if (value == 2) {
            return 1;
        }
        if (value == 3) {
            return 2;
        }
        int x1 = 0, x2 = 1, x3 = 0;
        for (int i = 2; i < (value + 1); i++) {
            x3 = x1 + x2;
            x1 = x2;
            x2 = x3;
        }
        return x3;
    }

    /**
     * Solves the Reduce operation with its Combine and Processing phases
     */
    public void solveCombineAndProcessing() {
        /* start with the Combine phase ... */
        for (Map<Integer, Integer> mappedResult : mappedResultsList) {
            /* ... and combine the results stored in a Map for the fragments of
             a file into a bigger Map specific to a file */
            for (Map.Entry<Integer, Integer> entry : mappedResult.entrySet()) {
                if (combinedMappedResults.containsKey(entry.getKey())) {
                    combinedMappedResults.put(entry.getKey(),
                            combinedMappedResults.get(entry.getKey()) + entry.getValue());
                } else {
                    combinedMappedResults.put(entry.getKey(), entry.getValue());
                }
            }
        }
        /* determine the length of the longest word in the entire file */
        maxLength = Collections.max(combinedMappedResults.keySet());
        /* combine the lists with the longest words within the
        fragments of a file in a bigger List specific to a file */
        for (List<String> longestWords : longestWordsList) {
            if (!longestWords.isEmpty() && longestWords.get(0).length() == maxLength) {
                combinedLongestWords.addAll(longestWords);
            }
        }

        /* continue with the Processing phase and get the number of words with the
        longest length and the total number of words within a file */
        maxLengthCount =  combinedLongestWords.size();
        float wordsCount = combinedMappedResults.values().stream().mapToInt(Integer::intValue).sum();
        /* determine the rank of a file */
        for (Map.Entry<Integer, Integer> entry : combinedMappedResults.entrySet()) {
            fileRank += getFibonacciValue(entry.getKey() + 1) * entry.getValue();
        }
        fileRank /= wordsCount;
    }

    public ReduceTask(String fileName) {
        this.fileName = fileName;
        mappedResultsList = new ArrayList<>();
        longestWordsList = new ArrayList<>();
        combinedMappedResults = new HashMap<>();
        combinedLongestWords = new ArrayList<>();
        fileRank = 0;
        maxLength = 0;
        maxLengthCount = 0;
    }

    public List<Map<Integer, Integer>> getMappedResultsList() {
        return mappedResultsList;
    }

    public List<List<String>> getLongestWordsList() {
        return longestWordsList;
    }

    /**
     * @return the name of the file from a file path
     */
    public String getFileName() {
        String[] parts = fileName.split("/");
        return parts[parts.length - 1];
    }

    public float getFileRank() {
        return fileRank;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public int getMaxLengthCount() {
        return maxLengthCount;
    }
}
