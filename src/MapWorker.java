import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * The class for a worker that solves only the Mask task of a fragment within a file
 */
public class MapWorker implements Runnable {
    private final int id;

    public MapWorker(int id) {
        this.id = id;
    }

    /**
     * Determines whether or not a byte is a white space
     * @param aByte byte to be verified
     * @return true if the given byte corresponds to the ASCII code of a white space, false otherwise
     */
    public boolean isWhiteSpace(byte aByte) {
        char ch = (char) aByte;
        return ch == ' ';
    }

    /**
     * Determines whether or not a byte is a letter
     * @param aByte byte to be verified
     * @return true if the given byte corresponds to the ASCII code of a letter, false otherwise
     */
    public boolean isLetter(byte aByte) {
        char ch = (char) aByte;
        return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
    }

    /**
     * Adjusts the fragment so that it always begins with and ends in a full word
     * @param taskId of the task that stores the fragment in question
     * @return the adjusted fragment
     * @throws IOException for the RandomAccessFile class
     */
    public String determineFragment(int taskId) throws IOException {
        int offset = (int) Main.mapTasksList.get(taskId).getOffset();
        int textSize = (int) Main.mapTasksList.get(taskId).getTextSize();
        int firstChPos = offset; /* get the position of the first character for the initial fragment*/
        int lastChPos = offset + textSize - 1;  /* same for the last character */

        /* create a random access file and, for now, load the whole content of the file into an array of bytes */
        RandomAccessFile file = new RandomAccessFile(Main.mapTasksList.get(taskId).getFileName(), "r");
        file.seek(0);
        byte[] initialBytes = new byte[(int) file.length()];
        file.read(initialBytes);

        /* check if the fragment starts with the middle of a word ... */
        if (firstChPos != 0 && isLetter(initialBytes[firstChPos]) && isLetter(initialBytes[firstChPos - 1])) {
            int chPos = firstChPos;
            /* ... if yes, move the position of the first character to the end of the first word ... */
            while (chPos < file.length() && isLetter(initialBytes[chPos])) {
                firstChPos++;
                chPos++;
            }
            /* ... and move the position again until the first non-whitespace character is found */
            if (firstChPos < file.length() && isWhiteSpace(initialBytes[firstChPos])) {
                while (chPos < file.length() && isWhiteSpace(initialBytes[chPos])) {
                    firstChPos++;
                    chPos++;
                }
            }
        }

        /* check if the fragment ends in the middle of a word ... */
        if (textSize == Main.fragmentSize && isLetter(initialBytes[lastChPos])
                && isLetter(initialBytes[lastChPos + 1])) {
            int chPos = lastChPos;
            /* ... if yes, move the position of the last character to the end of the last word ... */
            while (chPos < file.length() && isLetter(initialBytes[chPos])) {
                lastChPos++;
                chPos++;
            }
            lastChPos--;
        }

        /* reset the offset and the textSize to the newly adjusted values */
        offset = firstChPos;
        textSize = lastChPos - offset + 1;

        /* load only the newly adjusted fragment into an array of bytes */
        file.seek(offset);
        byte[] fragmentBytes = new byte[textSize];
        file.read(fragmentBytes);
        file.close();
        /* convert the array of bytes into a String and return it */
        return new String(fragmentBytes, StandardCharsets.UTF_8);
    }

    /**
     * Solves the Map operation after the adjustment of the fragment's offset and size.
     * Each worker gets only a part of the total number of tasks for the Map operation (see README.txt).
     */
    @Override
    public void run() {
        for (int i = id; i < Main.mapTasksList.size(); i += Main.numberOfWorkers) {
            try {
                Main.mapTasksList.get(i).solveFragment(determineFragment(i));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
