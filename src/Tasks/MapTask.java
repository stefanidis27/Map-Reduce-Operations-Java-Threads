package Tasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The class for a task that solves only the Map operation
 */
public class MapTask {
    private final String fileName;
    private final long offset;
    private final long textSize;
    private final Map<Integer, Integer> mappedResults;
    private List<String> longestWords;

    /**
     * Solves the Map operation by processing a given fragment of a file
     * @param fragmentString the fragment to be processed
     */
    public void solveFragment(String fragmentString) {
        /* split the fragment into words and create a list of words */
        String[] words = fragmentString.split("[;:/?~\\\\.,><'\\[\\]{}()!@#$%^&\\-_+’=*”|\n\r\t ]");
        List<String> wordsList = new ArrayList<>();
        Collections.addAll(wordsList, words);
        wordsList = wordsList.stream().filter(s -> s.length() != 0).collect(Collectors.toList());

        /* determine and store the lengths of the words and their occurrences within the fragment */
        for (String word : wordsList) {
            if (mappedResults.containsKey(word.length())) {
                mappedResults.put(word.length(),
                        mappedResults.get(word.length()) + 1);
            } else {
                mappedResults.put(word.length(), 1);
            }
        }

        if (!mappedResults.isEmpty()) {
            /* determine and store the list containing the longest words within a fragment */
            int maxLength = Collections.max(mappedResults.keySet());
            longestWords = wordsList.stream().filter(w -> w.length() == maxLength).collect(Collectors.toList());
        }
    }

    public MapTask(String fileName, long offset, long textSize) {
        this.fileName = fileName;
        this.offset = offset;
        this.textSize = textSize;
        mappedResults = new HashMap<>();
        longestWords = new ArrayList<>();
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
    }

    public long getTextSize() {
        return textSize;
    }

    public Map<Integer, Integer> getMappedResults() {
        return mappedResults;
    }

    public List<String> getLongestWords() {
        return longestWords;
    }
}
