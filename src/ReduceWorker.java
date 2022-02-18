/**
 * The class for a worker that solves only the Reduce task of a file
 */
public class ReduceWorker implements Runnable {
    private final int id;

    public ReduceWorker(int id) {
        this.id = id;
    }

    /**
     * Solves the Combine and Processing phases of the Reduce operation.
     * Each worker gets only a part of the total number of tasks for the Reduce operation (see README.txt).
     */
    @Override
    public void run() {
        for (int i = id; i < Main.reduceTasksList.size(); i += Main.numberOfWorkers) {
            Main.reduceTasksList.get(i).solveCombineAndProcessing();
        }
    }
}
