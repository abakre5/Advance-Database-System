package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.time.*;

import btree.BTreeFile;
import btree.FloatKey;
import btree.IndexFile;
import bufmgr.PagePinnedException;
import catalog.*;
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
    private static final short STR_SIZE = 32;

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

    private void printReadAndWrites() {
        System.out.println("Number of pages read: " + PageCounter.getReadCounter());
        System.out.println("Number of pages written: " + PageCounter.getWriteCounter());
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
        System.out.println("\u001b[2J\u001b[H");
    }
    private static boolean closeDB() {
        boolean status = OK;
        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (PagePinnedException e) {

        } catch (Exception e) {
            e.printStackTrace();
            status = FAIL;
        }

        try {
            SystemDefs.JavabaseDB.closeDB();
        } catch (IOException e) {
            System.err.println("*** error:" + e);
            status = FAIL;
        }

        return status;
    }

    private static void deleteDBFiles(String pathPrefix) {
        File directory = new File(pathPrefix);
        String filePattern = NAMEROOT + System.getProperty("user.name") + ".minibase-";
        File[] files = directory.listFiles();

        for (File file: files) {
            try {
                if (file.getName().contains(filePattern)) {
                    System.out.println("  Removed " + file.getCanonicalPath());
                    file.delete();
                }

            } catch (Exception e) {
                System.out.printf("Error: unable delete files %s/%s\n", pathPrefix, filePattern);
            }
        }
    }

    private static void deleteFile(String filename) {
        File f = new File(filename);
        if (f.isFile()) {
            f.delete();
        }
    }

    private static boolean openDB(String dbname) {
        boolean status = OK;
        String dbnamepath = dbpath + "-" + dbname.toLowerCase() + ".db";
        boolean dbExists = new File(dbnamepath).isFile();

        if (dbExists) {
            SystemDefs.MINIBASE_RESTART_FLAG = true;
        } else {
            // Kill anything that might be hanging around
            System.out.println("DB file does not exist... create one!");
            //deleteDBFiles("/tmp");
            deleteFile(logpath);
        }

        try {
            ExtendedSystemDefs sysdef = new ExtendedSystemDefs(dbnamepath, MINIBASE_DB_SIZE, NUMBUF, "Clock");
        } catch (Exception e) {
            e.printStackTrace();
            status = FAIL;
        }

        return status;
    }

    private static boolean isTableInDB(String tableName) {
        RelDesc record = new RelDesc();
        try {
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(tableName, record);
        } catch (Catalogrelnotfound e) {
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
    private static boolean readDataIntoHeapFile(String tableName, String filename, boolean createRel) {
        int numAttribs;
        AttrType[] attrTypes;
        boolean status = OK;
        String[] schemaInfo;
        RID rid;

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            line = br.readLine();
            /* Get the number of attributes from first line of input data file */
            numAttribs = Integer.parseInt(line.trim());
            System.out.println("Number of data attributes: " + numAttribs);
            attrTypes = new AttrType[numAttribs];

            /*
             * attrInfo[0] = attribute NAME
             * attrInfo[1] - attribute TYPE (STR/INT)
             */
            String[] fieldNames = new String[numAttribs];
            String[] attrInfo;
            schemaInfo = new String[numAttribs];
            int numStringAttr = 0;
            for (int i = 0; i < numAttribs; ++i) {
                schemaInfo[i] = br.readLine().trim();
                attrInfo = schemaInfo[i].split("\\s+");
                fieldNames[i] = attrInfo[0];

                /* TBD: store table scehema info */

                if (attrInfo[1].equalsIgnoreCase("INT")) {
                    attrTypes[i] = new AttrType(AttrType.attrInteger);
                } else {
                    attrTypes[i] = new AttrType(AttrType.attrString);
                    numStringAttr++;
                }
            }
            System.out.println("data" + Arrays.toString(schemaInfo));

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
                return status;
            }

            /* if query is create_table, create relation catalog */
            if (createRel) {
                try {
                    /* add relation catalog  */

                    int SIZE_OF_INT = 4;
                    attrInfo[] attrs = new attrInfo[numAttribs];

                    for (int i = 0; i < numAttribs; ++i) {
                        attrs[i] = new attrInfo();
                        attrs[i].attrType = new AttrType(attrTypes[i].attrType);
                        attrs[i].attrName = fieldNames[i];
                        attrs[i].attrLen = (attrTypes[i].attrType == AttrType.attrInteger) ? SIZE_OF_INT : STR_SIZE;
                    }

                    ExtendedSystemDefs.MINIBASE_RELCAT.createRel(tableName, numAttribs, attrs);
                } catch (Catalogrelexists e) {
                    System.err.println("*** error: table already exists: ");
                    status = FAIL;
                    return status;
                } catch (Exception e) {
                    System.err.println("*** error creating relation catalog: " + e);
                    e.printStackTrace();
                    status = FAIL;
                    return status;
                }
            } else {
                /* query is insert_data:
                 * ensure data file schema matches relation
                 * catalog before adding rows
                 */
                RelDesc record = new RelDesc();
                try {
                    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(tableName, record);
                } catch (Catalogrelnotfound e) {
                    System.err.printf("*** error relation '%s' not found\n", tableName);
                    status = FAIL;
                    return status;
                } catch (Exception e) {
                    e.printStackTrace();
                    return FAIL;
                }

                /* number of attributes per catalog */
                int numAttrCat;
                AttrDesc[] attrs = new AttrDesc[numAttribs];
                try {
                    numAttrCat = ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(tableName, 0, attrs);
                    /* if number of attributes from datafile doesn't match catalog */
                    if (numAttrCat != numAttribs) {
                        System.err.println("*** error: schema datafile and relation schema mismatch");
                        return FAIL;
                    }
                } catch (Catalogindexnotfound e) {
                    System.err.printf("*** error relation '%s' mismatch\n", tableName);
                    return FAIL;
                } catch (Exception e) {
                    System.err.println("*** error " + e);
                    e.printStackTrace();
                    status = FAIL;
                    return status;
                }

                /* check if datafile schema matches catalog scehma */
                int idx;
                for (int i = 0; i < numAttrCat; ++i) {
                    idx = attrs[i].attrPos - 1;
                    if (attrTypes[idx].attrType != attrs[i].attrType.attrType || !fieldNames[idx].equalsIgnoreCase(attrs[i].attrName)) {
                        System.err.println("*** error: schema datafile and relation schema mismatch");
                        return FAIL;
                    }
                }
            }

            Heapfile tableHeapFile;
            try {
                tableHeapFile = new Heapfile(tableName);
            } catch (Exception e) {
                System.err.println("*** error creating Heapfile");
                e.printStackTrace();
                status = FAIL;
                return status;
            }

            /*
            t = new Tuple(size);
            try {
                t.setHdr((short) numAttribs, attrTypes, strSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

             */

            int num_tuples = 0;
            while ((line = br.readLine()) != null) {
                /* read each line from the file, create tuple, and insert into DB */
                String row[] = line.trim().split("\\s+");
                //System.out.println(Arrays.toString(row));

                for (int i=0; i < numAttribs; ++i) {
                    try {
                        if (attrTypes[i].attrType == AttrType.attrInteger) {
                            t.setIntFld(i + 1, Integer.parseInt(row[i]));
                        } else {
                            t.setStrFld(i + 1, row[i]);
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
                    rid = tableHeapFile.insertRecord(t.getTupleByteArray());
                }
                catch (Exception e) {
                    System.err.println("*** error in Heapfile.insertRecord() ***");
                    status = FAIL;
                    e.printStackTrace();
                }

                ++num_tuples;
            }
            System.out.printf("Number of tuples added to table '%s': %d\n", tableName, num_tuples);

        } catch (FileNotFoundException e) {
            status = FAIL;
            System.err.println("*** error: datafile not found");
        } catch (IOException e) {
            status = FAIL;
            e.printStackTrace();
        }
        return status;
    }
    private static boolean createTable(String tableName, int indexType, int indexAttr, String filename) {
        boolean status = OK;

        status = readDataIntoHeapFile(tableName, filename, true);
        if (status == FAIL) {
            return status;
        }
        /* code to create clustered btree/hash index */
        if (indexType == IndexType.B_Index) {

        } else if (indexType == IndexType.Hash) {

        }

        /* flush pages to disk */
        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch(PagePinnedException e){

        } catch (Exception e) {
            System.err.println("*** error flushing pages to disk");
            e.printStackTrace();
            status = FAIL;
        }
        System.out.println();

        return status;
    }
    private static boolean insertIntoTable(String tableName, String fileName) {
        boolean status = readDataIntoHeapFile(tableName, fileName, false);
        if (status) {
            /* flush pages to disk */
            try {
                SystemDefs.JavabaseBM.flushAllPages();
            } catch(PagePinnedException e){

            } catch (Exception e) {
                System.err.println("*** error flushing pages to disk");
                e.printStackTrace();
                status = FAIL;
            }
            System.out.println();
        }
        return status;
    }

    private static boolean deleteFromTable(String tableName, String fileName) {
        boolean status = OK;
        int numAttribs;
        AttrType[] attrTypes;
        String[] schemaInfo;
        Heapfile tableFile = null;
        Scan scan = null;
        Tuple t = new Tuple();
        int numDelTuples = 0;
        List<Tuple> delTuples = new ArrayList<Tuple>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            line = br.readLine();
            /* Get the number of attributes from first line of input data file */
            numAttribs = Integer.parseInt(line.trim());
            System.out.println("Number of data attributes: " + numAttribs);
            attrTypes = new AttrType[numAttribs];

            /*
             * attrInfo[0] = attribute NAME
             * attrInfo[1] - attribute TYPE (STR/INT)
             */
            String[] fieldNames = new String[numAttribs];
            String[] attrInfo;
            schemaInfo = new String[numAttribs];
            int numStringAttr = 0;
            for (int i = 0; i < numAttribs; ++i) {
                schemaInfo[i] = br.readLine().trim();
                attrInfo = schemaInfo[i].split("\\s+");
                fieldNames[i] = attrInfo[0];

                if (attrInfo[1].equalsIgnoreCase("INT")) {
                    attrTypes[i] = new AttrType(AttrType.attrInteger);
                } else {
                    attrTypes[i] = new AttrType(AttrType.attrString);
                    numStringAttr++;
                }
            }
            System.out.println("delete_data_schema" + Arrays.toString(schemaInfo));

            short[] strSizes = new short[numStringAttr];
            for (int i = 0; i < strSizes.length; ++i) {
                strSizes[i] = STR_SIZE;
            }


            try {
                t.setHdr((short) numAttribs, attrTypes, strSizes);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
                return  status;
            }

            /*
             * ensure data file schema matches relation
             * catalog before adding rows
             */
            RelDesc record = new RelDesc();
            try {
                ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(tableName, record);
            } catch (Catalogrelnotfound e) {
                System.err.printf("*** error relation '%s' not found\n", tableName);
                status = FAIL;
                return status;
            } catch (Exception e) {
                e.printStackTrace();
                return FAIL;
            }

            /* number of attributes as per catalog */
            int numAttrCat;
            AttrDesc[] attrs = new AttrDesc[numAttribs];
            try {
                numAttrCat = ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(tableName, 0, attrs);
                /* if number of attributes from datafile doesn't match catalog */
                if (numAttrCat != numAttribs) {
                    System.err.println("*** error: schema datafile and relation schema mismatch");
                    return FAIL;
                }
            } catch (Catalogindexnotfound e) {
                System.err.printf("*** error relation '%s' mismatch\n", tableName);
                return FAIL;
            } catch (Exception e) {
                System.err.println("*** error " + e);
                e.printStackTrace();
                status = FAIL;
                return status;
            }

            /* check if datafile schema matches catalog schema */
            int idx;
            for (int i = 0; i < numAttrCat; ++i) {
                idx = attrs[i].attrPos - 1;
                if (attrTypes[idx].attrType != attrs[i].attrType.attrType ||
                        fieldNames[idx].equalsIgnoreCase(attrs[i].attrName) == false) {
                    System.err.println("*** error: schema datafile and relation schema mismatch");
                    return FAIL;
                }
            }

            try {
                tableFile = new Heapfile(tableName);
                scan = new Scan(tableFile);
            } catch (Exception e) {
                System.err.println("*** error creating Heapfile/scan");
                e.printStackTrace();
                status = FAIL;
                return status;
            }

            while ((line = br.readLine()) != null) {
                /* read each line from the file, create tuple, and insert into DB */
                String row[] = line.trim().split("\\s+");
                //System.out.println(Arrays.toString(row));

                for (int i=0; i < numAttribs; ++i) {
                    try {
                        if (attrTypes[i].attrType == AttrType.attrInteger) {
                            t.setIntFld(i + 1, Integer.parseInt(row[i]));
                        } else {
                            t.setStrFld(i + 1, row[i]);
                        }
                    }
                    catch (Exception e) {
                        System.err.println("*** Heapfile error in Tuple.setFloFld() ***");
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                /* insert tuple into a list */
                delTuples.add(new Tuple(t));
            }
            numDelTuples = delTuples.size();
            if (numDelTuples == 0) {
                return OK;
            }
            //System.out.printf("Number of tuples added to table '%s': %d\n", tableName, num_tuples);

        } catch (FileNotFoundException e) {
            status = FAIL;
            System.err.println("*** error: datafile not found");
        } catch (IOException e) {
            status = FAIL;
            e.printStackTrace();
        }
        if (status == FAIL) {
            return status;
        }

        /* iterate through table heapfile, delete a tuple
         * if it's present in delTuples list
         */
        RID rid = new RID();
        Tuple temp;
        int numRowsDeleted = 0;
        try {
            while (status && (temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                if (delTuples.isEmpty()) {
                    break;
                }
                if (delTuples.contains(t)) {
                    tableFile.deleteRecord(rid);
                    numRowsDeleted++;
                }
                //printTuple(t, attrTypes);
            }
        } catch (Exception e) {
            System.err.println(e);
            status = FAIL;
        }
        System.out.printf("Deleted %d rows from table '%s'\n", numRowsDeleted, tableName);

        return status;
    }

    private static void printTuple(Tuple t, AttrType[] attrTypes) {

        for (int i = 0; i < t.noOfFlds(); ++i) {
            if (attrTypes[i].attrType == AttrType.attrInteger) {
                try {
                    System.out.printf("%-10d", t.getIntFld(i + 1));
                } catch (Exception e) {
                    System.err.println(e);
                    return;
                }
            } else if (attrTypes[i].attrType == AttrType.attrString) {
                try {
                    System.out.printf("%-10s", t.getStrFld(i + 1));
                } catch (Exception e) {
                    System.err.println(e);
                    return;
                }
            }
        }
        System.out.println();
    }
    private static boolean outputTable(String tableName) {
        /* TBD: get schema info */

        boolean status = OK;
        Heapfile tableFile;
        Scan scan = null;

        if (isTableInDB(tableName) == false) {
            System.err.println("*** error: relation " + tableName + " not found in DB");
            return FAIL;
        }

        try {
            /* create an scan on the heapfile */
            tableFile = new Heapfile(tableName);
            scan = new Scan(tableFile);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }

        int numAttr = 0;
        RelDesc rec = new RelDesc();
        try {
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(tableName, rec);
            numAttr = rec.getAttrCnt();
            if (numAttr == 0) {
                System.err.println("*** error: catalog attribute count is 0 ");
                return FAIL;
            }
        } catch (Exception e) {
            System.err.println("*** error: " + e);
            return FAIL;
        }
        AttrType[] attrTypes = new AttrType[numAttr];
        for (int i = 0; i < attrTypes.length; ++i) {
            attrTypes[i] = new AttrType(AttrType.attrNull);
        }
        short[] strSizes = new short[numAttr];

        try {
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getTupleStructure(tableName, numAttr, attrTypes, strSizes);
        } catch (Exception e) {
            e.printStackTrace();
            return FAIL;
        }

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numAttr, attrTypes, strSizes);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }

        RID rid = new RID();
        String key = null;
        Tuple temp = null;

        /* print relation header */
        AttrDesc[] attrs = new AttrDesc[numAttr];
        try {
            int numAttrCat = ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(tableName, 0, attrs);
            /* if number of attributes from datafile doesn't match catalog */
            if (numAttrCat != numAttr) {
                System.err.println("*** error: schema datafile and relation schema mismatch");
                return FAIL;
            }
        } catch (Catalogindexnotfound e) {
            System.err.printf("*** error relation '%s' mismatch\n", tableName);
            return FAIL;
        } catch (Exception e) {
            System.err.println("*** error " + e);
            e.printStackTrace();
            status = FAIL;
            return status;
        }

        System.out.println();
        int pos = 0;
        for (int i = 0; pos < numAttr; ++i) {
            i = i % numAttr;
            if (attrs[i].attrPos - 1 == pos) {
                System.out.printf("%-10s", attrs[i].attrName);
                ++pos;
            }
        }
        System.out.println("\n----------------------------------");

        int rows = 0;
        try {
            while ((temp = scan.getNext(rid)) != null) {
                t.tupleCopy(temp);
                printTuple(t, attrTypes);
                ++rows;
            }
        } catch (Exception e) {
            System.err.println(e);
        }

        System.out.println("\nNumber of rows: " + rows);

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
                case "cls":
                case "clear": {
                    clearScreen();
                    break;
                }
                case "open_db":
                case "open_database": {
                    if (tokens.length < 2) {
                        System.out.println("Error: database name missing");
                        break;
                    }
                    dbName = new String(tokens[1]);
                    boolean status = dbOpen = openDB(dbName);
                    if (status == FAIL) {
                        dbName = "";
                        System.out.println("\nFailed to open database");
                    }
                    break;
                }
                case "close_db":
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
                    String clustered = "";
                    String index = "";
                    String indexAttr = "";
                    String datafile = "";

                    if (dbOpen == false) {
                        System.out.println("create_table: no database is open");
                        break;
                    }
                    if (tokens.length < 3) {
                        System.out.println("create_table: insufficent arguments");
                        break;
                    } else if (tokens.length >= 6) {
                        clustered = tokens[2];
                        index = tokens[3];
                        indexAttr = tokens[4];
                        datafile = tokens[5];
                        try {
                            if (clustered.equalsIgnoreCase("clustered") == false) {
                                System.out.println("create_table: only clustered index supported");
                                break;
                            } else if (index.equalsIgnoreCase("btree") == false &&
                                    index.equalsIgnoreCase("hash") == false) {
                                System.out.println("create_table: only btree/hash index supported");
                                break;
                            } else if (Integer.parseInt(indexAttr) < 1) {
                                System.out.println("create_table: ATT_NO should be >1");
                                break;
                            } else if (new File(datafile).isFile() == false) {
                                System.out.println("create_table: input datafile doesn't exist");
                                break;
                            }

                        } catch (Exception e) {
                            System.out.println("create_table: invalid ATT_NO " + tokens[3]);
                            break;
                        }

                    }

                    /* inputs sanitized. proceed */
                    boolean status      = OK;
                    int indexType       = IndexType.None;
                    String filename     = new String(tokens[2]);
                    String tableName    = new String(tokens[1]);
                    int indexKeyAttr    = 0;

                    if (clustered.equalsIgnoreCase("clustered")) {
                        filename = new String(datafile);
                        indexType = index.equalsIgnoreCase("btree") ? IndexType.B_Index : IndexType.Hash;
                        indexKeyAttr = Integer.parseInt(indexAttr);
                    }
                    status = createTable(tableName, indexType, indexKeyAttr, filename);
                    if (status == FAIL) {
                        //System.out.println("Error: create_table failed");
                    }

                    break;
                }
                case "output_table": {
                    if (dbOpen == false) {
                        System.out.println("output_table: no database is open");
                        break;
                    }
                    if (tokens.length < 2) {
                        System.out.println("output_table: insufficient arguments");
                        break;
                    }

                    String tableName = new String(tokens[1]);
                    boolean status = outputTable(tableName);
                    break;
                }
                case "insert_data": {
                    if (dbOpen == false) {
                        System.out.println("insert_data: no database is open");
                        break;
                    }
                    if (tokens.length < 3) {
                        System.out.println("insert_data: insufficient arguments");
                        break;
                    }
                    String tableName = tokens[1];
                    String filename = tokens[2];

                    boolean status = insertIntoTable(tableName, filename);

                    break;
                }
                case "delete_data": {
                    if (dbOpen == false) {
                        System.out.println("delete_data: no database is open");
                        break;
                    }
                    if (tokens.length < 3) {
                        System.out.println("delete_data: insufficient arguments");
                        break;
                    }
                    String tableName = tokens[1];
                    String filename = tokens[2];

                    boolean status = deleteFromTable(tableName, filename);

                    break;
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