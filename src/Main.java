import Tasks.MapTask;
import Tasks.ReduceTask;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {
    public static int numberOfWorkers;
    public static List<String> fileNamesList;
    public static int fragmentSize;
    public static int numberOfFiles;
    public static List<MapTask> mapTasksList = new ArrayList<>();
    public static List<ReduceTask> reduceTasksList = new ArrayList<>();

    /**
     * Reads from the input file and extracts the size of the fragment, the number of files and their paths
     * @param inFileName input fle to be read
     */
    public static void readFromFile(String inFileName) {
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(inFileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fragmentSize = scanner != null ? scanner.nextInt() : 0;
        numberOfFiles = scanner != null ? scanner.nextInt() : 0;
        fileNamesList = new ArrayList<>(numberOfFiles);
        if (scanner != null) {
            scanner.skip("\n");
            for (int i = 0; i < numberOfFiles; i++) {
                fileNamesList.add(scanner.nextLine());
            }
            scanner.close();
        }
    }

    /**
     * Writes the files names in order of their ranks
     * @param outFileName output file to be written
     */
    public static void writeInFile(String outFileName) {
        StringBuilder outFileText = new StringBuilder();
        for (ReduceTask reduceTask : reduceTasksList) {
            outFileText.append(reduceTask.getFileName());
            outFileText.append(',');
            outFileText.append(String.format("%.2f", reduceTask.getFileRank()));
            outFileText.append(',');
            outFileText.append(reduceTask.getMaxLength());
            outFileText.append(',');
            outFileText.append(reduceTask.getMaxLengthCount());
            outFileText.append('\n');
        }
        try {
            FileWriter fileWriter = new FileWriter(outFileName);
            fileWriter.write(outFileText.toString());
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args the number of workers, the input file path and the output file path
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: Main <workers> <in_file> <out_file>");
            return;
        }
        numberOfWorkers = Integer.parseInt(args[0]);
        String inFileName = args[1];
        String outFileName = args[2];

        readFromFile(inFileName);

        /* for each file ... */
        for (int i = 0; i < numberOfFiles; i++) {
            File file = new File(fileNamesList.get(i));
            /* ... determine the number of tasks that process a fragment with its size equal to the fragment size ... */
            long numberOfFullTasks = file.length() / fragmentSize;
            /* ... and the size of the last fragment ... */
            long partialTaskSize = file.length() % fragmentSize;
            /* ... and create the tasks for the Map operation accordingly */
            for (int j = 0; j < numberOfFullTasks; j++) {
                mapTasksList.add(new MapTask(fileNamesList.get(i), (long) j * fragmentSize, fragmentSize));
            }
            mapTasksList.add(new MapTask(fileNamesList.get(i), file.length() - partialTaskSize, partialTaskSize));
        }

        /* create the threads for the Map operation (MapWorker) and put them to work */
        Thread[] mapThreads = new Thread[numberOfWorkers];
        for (int i = 0; i < numberOfWorkers; i++) {
            mapThreads[i] = new Thread(new MapWorker(i));
            mapThreads[i].start();
        }
        for (int i = 0; i < numberOfWorkers; i++) {
            try {
                mapThreads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /* for each file ... */
        for (String fileName : fileNamesList) {
            /* ... create a task for the Reduce operation ... */
            reduceTasksList.add(new ReduceTask(fileName));
            /* ... and collect the results of all the fragments of a file in lists  */
            reduceTasksList.get(reduceTasksList.size() - 1).getMappedResultsList().addAll(
                    mapTasksList.stream().filter(t -> t.getFileName().equals(fileName)).map(
                            MapTask::getMappedResults).collect(Collectors.toList()));
            reduceTasksList.get(reduceTasksList.size() - 1).getLongestWordsList().addAll(
                    mapTasksList.stream().filter(t -> t.getFileName().equals(fileName)).map(
                            MapTask::getLongestWords).collect(Collectors.toList()));
        }

        /* create the threads for the Reduce operation (ReduceWorker) and put them to work */
        Thread[] reduceThreads = new Thread[numberOfWorkers];
        for (int i = 0; i < numberOfWorkers; i++) {
            reduceThreads[i] = new Thread(new ReduceWorker(i));
            reduceThreads[i].start();
        }
        for (int i = 0; i < numberOfWorkers; i++) {
            try {
                reduceThreads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /* sort the list of tasks for the Reduce operation by the rank of their files */
        reduceTasksList.sort((rt1, rt2) -> Float.compare(rt2.getFileRank(), rt1.getFileRank()));
        writeInFile(outFileName);
    }
}
