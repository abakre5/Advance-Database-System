package diskmgr;

/* A class to keep track of number of read and writes to pages */
public class PageCounter {
    private static boolean inited = false;
    /* number of reads */
    private static int readCounter;
    /* number of writes */
    private static int writeCounter;

    public static  boolean isInited() {
        return inited;
    }
    /* init function */
    public  static void init() {
        readCounter = writeCounter = 0;
        inited = true;
    }

    /* function to increment read counter */
    public static void readInc() {
        readCounter++;
    }
    /* function to increment write counter */
    public static void writeInc() {
        writeCounter++;
    }

    public static int getReadCounter() {
        return readCounter;
    }

    public static int getWriteCounter() {
        return writeCounter;
    }
}