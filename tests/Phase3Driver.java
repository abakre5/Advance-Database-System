package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.time.*;

import btree.BTreeFile;
import btree.FloatKey;
import btree.IndexFile;
import chainexception.ChainException;
import diskmgr.Page;
import diskmgr.PageCounter;
import global.*;
import heap.*;
import iterator.*;

public class Phase3Driver implements GlobalConst {

    /* class constants */
    private static final boolean OK = true;
    private static final boolean FAIL = false;
    private static final short STR_SIZE = 256;

    private static final String NAMEROOT = "phase3";

    private static final String dbpath = "/tmp/" + NAMEROOT + System.getProperty("user.name") + ".minibase-db";
    private static final String logpath = "/tmp/" + NAMEROOT + System.getProperty("user.name") + ".minibase-log";

    /* string list to store command history */
    private static List<String> prevCmds = new ArrayList<String>();
    private static void addCmdToHist(String cmd) {
        prevCmds.add(0, cmd);
    }
    private static String getPrevCmd() {
        if (prevCmds.isEmpty()) {
            return "";
        }
        String prevCmd;
        prevCmd = prevCmds.remove(0);
        prevCmds.add(prevCmd);
        return prevCmd;
    }

    private void printTuple(Tuple t) throws Exception {
        int num_fields = t.noOfFlds();
        for (int i=0; i < num_fields; ++i) {
            System.out.printf("%f ", t.getFloFld(i+1));
        }
        System.out.println("");
    }

    private void printReadAndWrites() {
        System.out.println("Number of pages read: " + PageCounter.getReadCounter());
        System.out.println("Number of pages written: " + PageCounter.getWriteCounter());
    }

    private  static  void usage() {
        System.out.println("\njava tests.Phase2Test " +
                "-datafile=<datafile> " +
                "-skyline=<nested | blocknested | sortfirst | btree | btreesort | all> " +
                "-attr=<comma separated numbers> " +
                "-npages=<number-of-pages>\n");
    }
    private static boolean skylineMethodValid(String skyline) {
        String[] validSkylines = {"nls", "bnls", "sfs", "bt", "bts"};
        for (String method : validSkylines) {
            if (method.equals(skyline)) {
                return true;
            }
        }
        return false;
    }

    private static void showHelp() {
        System.out.println(
                        "Supported commands:\n\n" +
                        "help/?: shows this menu\n\n" +
                        "prev: shows previously entered command\n\n" +
                        "exit/quit\n\n" +
                        "open_database DBNAME\n\n" +
                        "close_database\n\n" +
                        "create_table TABLENAME [CLUSTERED BTREE/HASH ATT_NO] FILENAME\n\n" +
                        "create_index BTREE/HASH ATT_NO TABLENAME\n\n" +
                        "insert_data TABLENAME FILENAME\n\n" +
                        "delete_data TABLENAME FILENAME\n\n" +
                        "output_table TABLENAME\n\n" +
                        "output_index TABLENAME ATT_NO\n\n" +
                        "skyline NLS/BNLS/SFS/BTS/BTS <comma-separate attributes-numbers> TABLENAME NPAGES [MATER OUTTABLENAME]\n\n" +
                        "groupby SORT/HASH  MAX/MIN/AGG/SKY  G_ATT_NO{A_ATT_NO1 . . . A_ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]\n\n" +
                        "join NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP NPAGES [MATER OUTTABLENAME]\n\n" +
                        "topkjoin HASH/NRA K OTABLENAME O_J_ATT_NO O_M_ATT_NO ITABLENAME I_JaTT_NO I_MaTT_NO NPAGES [MATER OUTTABLENAME]\n\n"
        );
    }

    private static void clearScreen() {
        System.out.print("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n" +
                "\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n" +
                "\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n" +
                "\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n" +
                "\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n");
    }
    private static boolean closeDB() {
        boolean status = OK;
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception e) {
            e.printStackTrace();
            status = FAIL;
        }

        return status;
    }
    private static boolean openDB(String dbname) {
        boolean status = OK;
        String dbnamepath = dbpath + "-" + dbname.toLowerCase();
        boolean dbExists = new File(dbnamepath).isFile();

        if (dbExists) {
            SystemDefs.MINIBASE_RESTART_FLAG = true;
        } else {

            System.out.println("DB file does not exist.. clear other files!!");
            // Kill anything that might be hanging around
            String newdbpath;
            String newlogpath;
            String remove_logcmd;
            String remove_dbcmd;
            String remove_cmd = "/bin/rm -rf ";

            newdbpath = dbpath + "*";
            newlogpath = logpath;

            remove_logcmd = remove_cmd + logpath;
            remove_dbcmd = remove_cmd + dbpath + "*";

            // Commands here is very machine dependent.  We assume
            // user are on UNIX system here.  If we need to port this
            // program to other platform, the remove_cmd have to be
            // modified accordingly.
            try {
                Runtime.getRuntime().exec(remove_logcmd);
                Runtime.getRuntime().exec(remove_dbcmd);
            } catch (IOException e) {
                System.err.println("" + e);
            }

            remove_logcmd = remove_cmd + newlogpath;
            remove_dbcmd = remove_cmd + newdbpath;

            //This step seems redundant for me.  But it's in the original
            //C++ code.  So I am keeping it as of now, just in case
            //I missed something
            try {
                Runtime.getRuntime().exec(remove_logcmd);
                Runtime.getRuntime().exec(remove_dbcmd);
            } catch (IOException e) {
                System.err.println("" + e);
            }
        }

        try {
            SystemDefs sysdef = new SystemDefs(dbnamepath, MINIBASE_DB_SIZE, NUMBUF, "Clock");
        } catch (Exception e) {
            e.printStackTrace();
            status = FAIL;
        }

        return status;
    }
    private static boolean readFileIntoHeapFile(String tableName, String filename) {
        int numAttribs;
        AttrType[] attrTypes;
        boolean status = OK;

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            line = br.readLine();
            numAttribs = Integer.parseInt(line.trim());
            System.out.println("Number of data attributes: " + numAttribs);
            attrTypes = new AttrType[numAttribs];

            String[] attrInfo;
            int numStringAttr = 0;
            for (int i = 0; i < numAttribs; ++i) {
                attrInfo = br.readLine().trim().split("\\s+");

                /* TBD: store table scehema info */

                if (attrInfo[1].equalsIgnoreCase("INT")) {
                    attrTypes[i] = new AttrType(AttrType.attrInteger);
                } else {
                    attrTypes[i] = new AttrType(AttrType.attrString);
                    numStringAttr++;
                }
            }

            short[] strSizes = new short[numStringAttr];
            for (int i = 0; i < strSizes.length; ++i) {
                strSizes[i] = STR_SIZE;
            }


            Tuple t = new Tuple();
            try {
                t.setHdr((short) numAttribs, attrTypes, strSizes);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            int size = t.size();

            RID rid;
            Heapfile tableHeapFile = null;
            try {
                tableHeapFile = new Heapfile(tableName);
            }
            catch (Exception e) {
                System.err.printf("*** error constructing Heapfile '%s' ***\n", tableName);
                status = FAIL;
                e.printStackTrace();
            }

            t = new Tuple(size);
            try {
                t.setHdr((short) numAttribs, attrTypes, strSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            int num_tuples = 0;
            while ((line = br.readLine()) != null) {
                /* read each line from the file, create tuple, and insert into DB */
                String attrStr[] = line.trim().split("\\s+");


                for (int i=0; i < numAttribs; ++i) {
                    try {
                        if (attrTypes[i].attrType == AttrType.attrInteger) {
                            t.setIntFld(i+1, Integer.parseInt(attrStr[i]));
                        } else {
                            t.setStrFld(i + 1, attrStr[i]);
                        }
                    }
                    catch (Exception e) {
                        System.err.println("*** Heapfile error in Tuple.setFloFld() ***");
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                /* insert tuple into heapfile */
                try {
                    rid = tableHeapFile.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    System.err.println("*** error in Heapfile.insertRecord() ***");
                    status = FAIL;
                    e.printStackTrace();
                }

                ++num_tuples;
            }

            System.out.printf("Number of tuples added to table '%s': %d\n", tableName, num_tuples);
        }
        catch (IOException e) {
            status = FAIL;
            e.printStackTrace();
        }
        return status;
    }
    private static boolean createTable(int indexType, String tableName, String filename) {
        boolean status = OK;

        /* TBD: store schema info */

        status = readFileIntoHeapFile(tableName, filename);
        if (status == FAIL) {
            return status;
        }
        /* code to create clustered btree/hash index */
        if (indexType == IndexType.B_Index) {

        } else if (indexType == IndexType.Hash) {

        }

        return status;
    }
    private static boolean outputTable(String tableName) {
        /* TBD: get schema info */

        boolean status = OK;
        Heapfile tableFile;
        Scan scan = null;

        try {
            /* create an scan on the heapfile */
            tableFile = new Heapfile(tableName);
            scan = new Scan(tableFile);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }

        RID rid = new RID();
        String key = null;
        Tuple temp = null;

        /*
        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        while ((temp = scan.getNext(rid)) != null) {
            t.tupleCopy(temp);

            try {
                key = t.getStrFld(2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                btf.insert(new StringKey(key), rid);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                temp = scan.getNext(rid);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

         */

        // close the file scan
        scan.closescan();
        return status;

    }
    private static void dbShell() throws java.io.IOException
    {
        String commandLine;
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        String[] tokens;
        boolean addCmdToHist = false;
        boolean dbOpen = false;
        String dbName = "";

        // Break with Ctrl+C
        while (true) {
            //read the command
            System.out.print(dbName + "$ ");
            commandLine = console.readLine().trim();

            // if just a return, loop
            if (commandLine.equals("")) {
                continue;
            }

            addCmdToHist = true;
            tokens = commandLine.split("\\s+");
            switch (tokens[0].toLowerCase()) {
                case "?":
                case "help": {
                    showHelp();
                    break;
                }
                case "clear": {
                    clearScreen();
                    break;
                }
                case "open_database": {
                    if (tokens.length < 2) {
                        System.out.println("Error: database name missing");
                        break;
                    }
                    dbName = new String(tokens[1]);
                    boolean status = dbOpen = openDB(tokens[1]);
                    if (status == FAIL) {
                        dbName = "";
                        System.out.println("\nFailed to open database.");
                    }
                    break;
                }
                case "close_database": {
                    boolean status;
                    if (dbOpen) {
                        status = closeDB();
                        if (status == FAIL) {
                            System.out.println("Error closing database...");
                        }
                        dbOpen = false;
                        dbName = "";
                    } else {
                        System.out.println("No database is open...");
                    }
                    break;
                }
                case "create_table": {
                    if (dbOpen == false) {
                        System.out.println("create_table: no database is open");
                        break;
                    }
                    if (tokens.length < 3) {
                        System.out.println("create_table: invalid command");
                        break;
                    } else if (tokens.length >= 6) {
                        try {
                            if (tokens[2].toLowerCase() != "clustered") {
                                System.out.println("create_table: only clustered index supported");
                                break;
                            } else if (tokens[3].equalsIgnoreCase("btree") == false &&
                                    tokens[3].equalsIgnoreCase("hash") == false) {
                                System.out.println("create_table: only btree/hash index supported");
                                break;
                            } else if (Integer.parseInt(tokens[4]) < 1) {
                                System.out.println("create_table: ATT_NO should be >1");
                                break;
                            } else if (new File(tokens[5]).isFile() == false) {
                                System.out.println("create_table: input datafile doesn't exist");
                                break;
                            }

                        } catch (Exception e) {
                            System.out.println("create_table: invalid ATT_NO " + tokens[3]);
                            break;
                        }

                    }

                    /* inputs sanitized. proceed */
                    boolean status = OK;
                    int indexType = IndexType.None;
                    String filename = new String(tokens[2]);
                    String tableName = new String(tokens[1]);

                    if (tokens[1].equalsIgnoreCase("clustered")) {
                        filename = new String(tokens[5]);

                        if (tokens[2].equalsIgnoreCase("btree")) {
                            indexType = IndexType.B_Index;
                        } else {
                            indexType = IndexType.Hash;
                        }
                    }
                    status = createTable(indexType, tableName, filename);

                    break;
                }
                case "output_table": {
                    if (dbOpen == false) {
                        System.out.println("output_table: no database is open");
                        break;
                    }
                    if (tokens.length < 2) {
                        System.out.println("output_table: invalid command");
                        break;
                    }
                    String tableName = new String(tokens[1]);
                    boolean status = outputTable(tableName);

                }
                case "prev": {
                    System.out.println(getPrevCmd());
                    continue;

                }
                case "quit":
                case "exit":
                {
                    return;
                }
                default: {
                    addCmdToHist = false;
                    System.out.println("Unsupported command: " + tokens[0]);
                    System.out.println("Enter 'help' or '?' to see supported commands");
                    break;
                }
            }
            if (addCmdToHist) {
                addCmdToHist(commandLine);
            }

        }

    }

    public static void main(String[] args) {
        boolean dbstatus = true;

        try {
            dbShell();
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}