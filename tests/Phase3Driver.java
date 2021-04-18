package tests;

import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import catalog.*;
import diskmgr.PageCounter;
import global.*;
import heap.*;
import iterator.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        flushToDisk();

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

        for (File file : files) {
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

    public static boolean isTableInDB(String tableName) {
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

        System.out.println(new File(filename).getAbsoluteFile());

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            line = br.readLine();
            /* Get the number of attributes from first line of input data file */
            //System.out.println("Line one -> " + Integer.parseInt(line.split(",")[0]));
            numAttribs = Integer.parseInt(line.split(",")[0]);
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
                attrInfo = schemaInfo[i].split(",");
                fieldNames[i] = attrInfo[0];

                /* TBD: store table scehema info */

                if (attrInfo[1].equalsIgnoreCase("INT")) {
                    attrTypes[i] = new AttrType(AttrType.attrInteger);
                } else if(attrInfo[1].equalsIgnoreCase("FLOAT")) {
                    attrTypes[i] = new AttrType(AttrType.attrReal);
                } else {
                    attrTypes[i] = new AttrType(AttrType.attrString);
                    numStringAttr++;
                }
            }
            System.out.println("data" + Arrays.toString(attrTypes));
            System.out.println("Fields" + Arrays.toString(fieldNames));

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
                int numAttrCat = record.getAttrCnt();
                AttrDesc[] attrs = new AttrDesc[numAttrCat];
                try {
                    numAttrCat = ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(tableName, 0, attrs);
                    /* if number of attributes from datafile doesn't match catalog */
                    if (numAttrCat != numAttribs) {
                        System.err.println("*** error: datafile schema and relation schema mismatch");
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
                        System.err.println("*** error: datafile schema and relation schema mismatch");
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
                String row[] = line.trim().split(",");
                System.out.println(Arrays.toString(row));

                for (int i = 0; i < numAttribs; ++i) {
                    try {
                        if (attrTypes[i].attrType == AttrType.attrInteger) {
                            t.setIntFld(i + 1, Integer.parseInt(row[i]));
                        } else if (attrTypes[i].attrType == AttrType.attrReal) {
                            t.setFloFld(i + 1, Float.parseFloat(row[i]));
                        } else {
                            t.setStrFld(i + 1, row[i]);
                        }
                    } catch (Exception e) {
                        System.err.println("*** Heapfile error in Tuple.setFloFld() ***");
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                /* insert tuple into heapfile */
                try {
                    rid = tableHeapFile.insertRecord(t.getTupleByteArray());
                } catch (Exception e) {
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
        status = flushToDisk();
        System.out.println();

        return status;
    }

    private static boolean insertIntoTable(String tableName, String fileName) {
        boolean status = readDataIntoHeapFile(tableName, fileName, false);
        if (status) {
            /* flush pages to disk */
            status = flushToDisk();
            System.out.println();
        }
        return status;
    }

    private static boolean flushToDisk() {
        boolean status = OK;
        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (PagePinnedException e) {

        } catch (Exception e) {
            System.err.println("*** error flushing pages to disk");
            e.printStackTrace();
            status = FAIL;
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
            //System.out.println("Number of data attributes: " + numAttribs);
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
                return status;
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
            int numAttrCat = record.getAttrCnt();
            AttrDesc[] attrs = new AttrDesc[numAttrCat];
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
                scan = tableFile.openScan();
            } catch (Exception e) {
                System.err.println("*** error creating Heapfile/scan");
                e.printStackTrace();
                status = FAIL;
                return status;
            }

            try {
                if (tableFile.getRecCnt() == 0) {
                    System.out.printf("No records in table '%s'\n", tableName);
                    scan.closescan();
                    return OK;
                }
            } catch (Exception e) {
                System.err.println("*** error: " + e);
                return FAIL;
            }

            while ((line = br.readLine()) != null) {
                /* read each line from the file, create tuple, and insert into DB */
                String row[] = line.trim().split("\\s+");
                //System.out.println(Arrays.toString(row));

                for (int i = 0; i < numAttribs; ++i) {
                    try {
                        if (attrTypes[i].attrType == AttrType.attrInteger) {
                            t.setIntFld(i + 1, Integer.parseInt(row[i]));
                        } else {
                            t.setStrFld(i + 1, row[i]);
                        }
                    } catch (Exception e) {
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
            System.out.printf("Number of tuples in delete file %d\n", numDelTuples);

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
        boolean tupleDel = false;
        int numRowsDeleted = 0;
        try {
            while (!delTuples.isEmpty() && status && ((temp = scan.getNext(rid)) != null)) {
                t.tupleCopy(temp);
                if (delTuples.isEmpty()) {
                    break;
                }
                //System.err.println("tuple(rid=" + rid.hashCode() + ") equality: " + t.equals(delTuples.get(0)));
                if (delTuples.contains(t)) {
                    tupleDel = tableFile.deleteRecord(rid);
                    delTuples.remove(t);
                    numRowsDeleted++;
                }
            }
        } catch (Exception e) {
            System.err.println("*** error: " + e);
            e.printStackTrace();
            status = FAIL;
        }

        scan.closescan();

        if (status && (numRowsDeleted != 0)) {
            /* flush pages to disk */
            status = flushToDisk();
        }
        System.out.printf("Deleted %d rows from table '%s'\n", numRowsDeleted, tableName);

        return status;
    }

    private static void printTuple(Tuple t, AttrType[] attrTypes) {
        for (int i = 0; i < t.noOfFlds(); ++i) {
            if (attrTypes[i].attrType == AttrType.attrInteger) {
                try {
                    System.out.printf("%-20d", t.getIntFld(i + 1));
                } catch (Exception e) {
                    System.err.println(e);
                    return;
                }
            } else if (attrTypes[i].attrType == AttrType.attrString) {
                try {
                    System.out.printf("%-20s", t.getStrFld(i + 1));
                } catch (Exception e) {
                    System.err.println(e);
                    return;
                }
            } else if (attrTypes[i].attrType == AttrType.attrReal) {
                try {
                    System.out.printf("%-20f", t.getFloFld(i + 1));
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

        if (!isTableInDB(tableName)) {
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
                System.out.printf("%-20s", attrs[i].attrName);
                ++pos;
            }
        }
        System.out.println();
        for (int i = 0; i < numAttr; ++i) {
            System.out.print("--------------------");
        }
        System.out.println();

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

    private static boolean createUnclusteredIndex(String tableName, int indexType, int indexAttr) {
        boolean status = OK;
        if (isTableInDB(tableName) == false) {
            System.err.println("*** error: relation " + tableName + " not found in DB");
            return FAIL;
        }

        if (indexType == IndexType.B_Index) {
            //ExtendedSystemDefs.MINIBASE_INDCAT.addIndex();
        } else {

        }
        return status;
    }
    private static void dbShell() throws Exception
    {
        String commandLine;
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        String[] tokens;
        boolean addCmdToHist = false;
        boolean dbOpen = false;
        String dbName = "";
        String cmd = "";

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
            cmd = tokens[0].toLowerCase();
            switch (cmd) {
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
                    } else if (tokens.length == 6) {
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
                            }
                            try {
                                if (Integer.parseInt(indexAttr) < 1) {
                                    System.out.println("create_table: ATT_NO should be >0");
                                    break;
                                }
                            } catch (Exception e) {
                                System.out.println("create_table: index ATT_NO is NaN: " + indexAttr);
                            }

                            if (new File(datafile).isFile() == false) {
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
                    if (!dbOpen) {
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

                    boolean status = deleteDataFromTable(tableName, filename);

                    break;
                }
                case "create_index": {
                    if (dbOpen == false) {
                        System.out.println(cmd + ": no database is open");
                        break;
                    }
                    if (tokens.length < 4) {
                        System.out.println(cmd + ": insufficient arguments");
                        break;
                    }
                    String indexTypeStr = tokens[1];
                    String indexAttrStr = tokens[2];
                    String tableName = tokens[3];

                    if (!indexTypeStr.equalsIgnoreCase("btree") &&
                            !indexTypeStr.equalsIgnoreCase("hash")) {
                        System.out.println("create_index: only btree/hash index supported");
                        break;
                    }
                    try {
                        if (Integer.parseInt(indexAttrStr) < 1) {
                            System.out.println("create_index: index ATT_NO should be >0");
                        }
                    } catch (Exception e) {
                        System.out.println("create_index: index ATT_NO is NaN: " + indexAttrStr);
                    }

                    int indexType = indexTypeStr.equalsIgnoreCase("btree") ? IndexType.B_Index : IndexType.Hash;
                    int indexAttr = Integer.parseInt(indexAttrStr);
                    createUnclusteredIndex(tableName, indexType, indexAttr);

                    break;
                }
                case "prev": {
                    System.out.println(getPrevCmd());
                    continue;

                }
                case "quit":
                case "exit": {
                    return;
                }
                case "groupby": {
                    System.out.println((java.util.Arrays.toString(tokens)));
                    performGroupBy(tokens);
                    System.out.println("---------------------------------------------------------------------------------");
                    break;
                }
                case "skyline":
                    System.out.println((java.util.Arrays.toString(tokens)));
                    performSkyline(tokens);
                    break;
                case "join": {
                    if (!dbOpen) {
                        System.out.println(cmd + ": no database is open");
                        break;
                    }
                    if (tokens.length < 8) {
                        System.out.println(cmd + ": insufficient arguments");
                        break;
                    }
                    boolean status = performJoin(tokens);
                    break;
                }

//    – TOPKJOIN HASH/NRA K OTABLENAME O J ATT NO O M ATT NO ITABLENAME I JATT NO I MATT NO NPAGES
//[MATER OUTTABLENAME]
                case "topkjoin": {
                    System.out.println((java.util.Arrays.toString(tokens)));
                    if (tokens.length < 10) {
                        System.out.println("Some arguments are missing");
                        break;
                    }
                    if (Integer.parseInt(tokens[2]) <= 0) {
                        System.out.println("The value of k must be more than or equal to 1");
                        break;
                    }
                    performTopK(tokens);
                    break;
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

    private static void performSkyline(String[] tokens) throws Exception {
        String typeOfSkyline = tokens[1];
        int[] prefList = getAggList(tokens[2]);
        String tableName = tokens[3];
        int nPages = Integer.parseInt(tokens[4]);
        String materTableName = null;
        if (tokens.length > 5 ){
            materTableName = tokens[6];
        }

        IteratorDesc iteratorDesc = Phase3Utils.getTableItr(tableName);

        switch (typeOfSkyline.toLowerCase()) {
            case "nls":
                break;
            case "bnls":
                assert iteratorDesc != null;
                BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(iteratorDesc.getAttrType(), iteratorDesc.getNumAttr(),
                        iteratorDesc.getStrSizes(), iteratorDesc.getScan(), tableName, prefList, prefList.length,nPages);
                assert materTableName != null;
                blockNestedLoopsSky.printSkyline(materTableName);
                blockNestedLoopsSky.close();
                break;
            case "SFS":
                break;
            case "BTS":
                break;
            case "BTSS":
                break;
            default:
                System.out.println("Select skyline operator from the specified list ...");
                return;
        }
    }

    //    – TOPKJOIN HASH/NRA K OTABLENAME O J ATT NO O M ATT NO ITABLENAME I JATT NO I MATT NO NPAGES
//[MATER OUTTABLENAME]
    private static void performTopK(String[] tokens) {
        boolean isHashBased = tokens[1].equalsIgnoreCase("HASH");
        int k = Integer.parseInt(tokens[2]);
        String outTableName = tokens[3];
        int outJoinAttrNumber = Integer.parseInt(tokens[4]);
        int outMergeAttrNumber = Integer.parseInt(tokens[5]);
        String innerTableName = tokens[6];
        int innerJoinAttrNumber = Integer.parseInt(tokens[7]);
        int innerMergeAttrNumber = Integer.parseInt(tokens[8]);
        int nPages = Integer.parseInt(tokens[9]);
        String materialTableName = null;
        if (tokens.length == 11){
             materialTableName = tokens[10];
        }
        System.out.println("K : " + k + " \noutTableName = " + outTableName + " \noutJoinAttr = " + outJoinAttrNumber +
                " \noutMergeAttr = " + outMergeAttrNumber
                + " \ninnerTableName: " + innerTableName + " innerJoinAttr: " + innerJoinAttrNumber +
                " \ninnerMergeAttr: " + innerMergeAttrNumber +
                " \nnPages : " + nPages + " \nmaterialTableName : " + materialTableName);

        IteratorDesc outIteratorDesc = null;
        try {
            outIteratorDesc = Phase3Utils.getTableItr(outTableName);
        } catch (HFException | HFBufMgrException | HFDiskMgrException | InvalidTupleSizeException | FileScanException | TupleUtilsException | InvalidRelation | IOException | PredEvalException | JoinsException | FieldNumberOutOfBoundException | PageNotReadException | InvalidTypeException | WrongPermat | UnknowAttrType e) {
            e.printStackTrace();
        }


        IteratorDesc innerIteratorDesc = null;
        try {
            innerIteratorDesc = Phase3Utils.getTableItr(innerTableName);
        } catch (HFException | HFBufMgrException | HFDiskMgrException | InvalidTupleSizeException | FileScanException | TupleUtilsException | InvalidRelation | IOException | PredEvalException | JoinsException | FieldNumberOutOfBoundException | PageNotReadException | InvalidTypeException | WrongPermat | UnknowAttrType e) {
            e.printStackTrace();
        }

        FldSpec outJoinAttr = new FldSpec(new RelSpec(RelSpec.outer), outJoinAttrNumber);
        FldSpec outMrgAttr = new FldSpec(new RelSpec(RelSpec.outer), outMergeAttrNumber);
        FldSpec innerJoinAttr = new FldSpec(new RelSpec(RelSpec.outer), innerJoinAttrNumber);
        FldSpec innerMrgAttr = new FldSpec(new RelSpec(RelSpec.outer), innerMergeAttrNumber);
        if (isHashBased){
            System.out.println("Hash based top k join is performed : ");

            try {
                new HashJoin5a(outIteratorDesc.getAttrType(), outIteratorDesc.getNumAttr(), outIteratorDesc.getStrSizes(),
                        outJoinAttr, outMrgAttr,
                        innerIteratorDesc.getAttrType(), innerIteratorDesc.getNumAttr(), innerIteratorDesc.getStrSizes(),
                        innerJoinAttr, innerMrgAttr,
                        outTableName, innerTableName, k, nPages);

            } catch (IOException | NestedLoopException | HashJoinException e) {
                e.printStackTrace();
            }
        } else{
            System.out.println("NRA based top k join algorithm is performed : ");
            AttrType x = outIteratorDesc.getAttrType()[outJoinAttr.offset - 1];
            if (x.attrType == AttrType.attrString){
                System.out.println("Join Attribute is of String type");
                try {
                    new TopK_NRAJoinString(outIteratorDesc.getAttrType(), outIteratorDesc.getNumAttr(), outIteratorDesc.getStrSizes(),
                            outJoinAttr, outMrgAttr,
                            innerIteratorDesc.getAttrType(), innerIteratorDesc.getNumAttr(), innerIteratorDesc.getStrSizes(),
                            innerJoinAttr, innerMrgAttr,
                            outTableName, innerTableName, k, nPages);

                } catch (IOException | FileScanException | InvalidRelation | TupleUtilsException | WrongPermat | InvalidTypeException | PageNotReadException | FieldNumberOutOfBoundException | PredEvalException | UnknowAttrType | InvalidTupleSizeException | JoinsException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    new TopK_NRAJoin(outIteratorDesc.getAttrType(), outIteratorDesc.getNumAttr(), outIteratorDesc.getStrSizes(),
                            outJoinAttr, outMrgAttr,
                            innerIteratorDesc.getAttrType(), innerIteratorDesc.getNumAttr(), innerIteratorDesc.getStrSizes(),
                            innerJoinAttr, innerMrgAttr,
                            outTableName, innerTableName, k, nPages);

                } catch (IOException | FileScanException | InvalidRelation | TupleUtilsException | WrongPermat | InvalidTypeException | PageNotReadException | FieldNumberOutOfBoundException | PredEvalException | UnknowAttrType | InvalidTupleSizeException | JoinsException e) {
                    e.printStackTrace();
                }
            }

        }




    }

    private static void performGroupBy(String[] tokens) {
        /**
         * Collect arguments
         */
        boolean isSort = tokens[1].equals("SORT");
        String aggOperator = tokens[2].toLowerCase();
        int groupByAttr = Integer.parseInt(tokens[3]);
        String aggListNonNormalized = tokens[4];
        String tableName = tokens[5];
        int nPages = Integer.parseInt(tokens[6]);
        String isMater = "";
        String tableNameT = "";
        if (tokens.length > 7) {
            isMater = tokens[7];
            tableNameT = tokens[8];
        }

        AggType aggType = getGroupByAggOperatorType(aggOperator);
        int[] aggList = getAggList(aggListNonNormalized);

        IteratorDesc iteratorDesc = null;
        try {
            iteratorDesc = Phase3Utils.getTableItr(tableName);
        } catch (HFException | HFBufMgrException | HFDiskMgrException | InvalidTupleSizeException | FileScanException | TupleUtilsException | InvalidRelation | IOException | PredEvalException | JoinsException | FieldNumberOutOfBoundException | PageNotReadException | InvalidTypeException | WrongPermat | UnknowAttrType e) {
            e.printStackTrace();
        }

        RelSpec relSpec = new RelSpec(RelSpec.outer);
        FldSpec groupByAttrFldSpec = new FldSpec(relSpec, groupByAttr);
        FldSpec[] aggListFldSpec = new FldSpec[aggList.length];
        for (int i = 0; i < aggListFldSpec.length; i++) {
            aggListFldSpec[i] = new FldSpec(relSpec, aggList[i]);
        }

        if (isSort) {
            GroupBywithSort groupBywithSort = null;
            try {
                assert iteratorDesc != null;
                groupBywithSort = new GroupBywithSort(iteratorDesc.getAttrType(),
                        iteratorDesc.getNumAttr(), iteratorDesc.getStrSizes(), iteratorDesc.getScan(), groupByAttrFldSpec, aggListFldSpec,
                        aggType, iteratorDesc.getProjlist(), iteratorDesc.getNumAttr(), nPages, tableNameT);
                groupBywithSort.getAggregateResult();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    assert groupBywithSort != null;
                    groupBywithSort.close();
                } catch (HFDiskMgrException | InvalidTupleSizeException | IOException | InvalidSlotNumberException | HFBufMgrException | FileAlreadyDeletedException e) {
                    System.out.println("Warning ->  Error occurred while closing Group By with Sort operation -> " + e.getMessage());
                }
            }
        } else {
            try {
                assert iteratorDesc != null;
                GroupBywithHash groupBywithHash = new GroupBywithHash(iteratorDesc.getAttrType(),
                        iteratorDesc.getNumAttr(), iteratorDesc.getStrSizes(), tableName, groupByAttrFldSpec, aggListFldSpec, aggType,
                        iteratorDesc.getProjlist(), iteratorDesc.getNumAttr(), nPages, tableNameT);
                groupBywithHash.getAggregateResult();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        assert iteratorDesc != null;
        iteratorDesc.getScan().close();
    }

    private static AggType getGroupByAggOperatorType(String aggOperator) {
        AggType aggType = null;
        switch (aggOperator) {
            case "min":
                aggType = new AggType(AggType.aggMin);
                break;
            case "max":
                aggType = new AggType(AggType.aggMax);
                break;
            case "avg":
                aggType = new AggType(AggType.aggAvg);
                break;
            case "sky":
                aggType = new AggType(AggType.aggSkyline);
                break;
            default:
                System.out.println("Please enter a valid agg operator[MIN, MAX, AVG, SKY]");
                break;

        }
        return aggType;
    }

    private static int[] getAggList(String aggListNonNormalized) {
        String[] aggListStr = aggListNonNormalized.split(",");
        int[] aggList = new int[aggListStr.length];
        for (int i = 0; i < aggListStr.length; i++) {
            aggList[i] = Short.parseShort(aggListStr[i]);
        }
        return aggList;
    }


    private static boolean deleteDataFromTable(String tableName, String filename) {
        boolean status = false;
        try {
            status = deleteData(tableName, filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (status) {
            /* flush pages to disk */
            try {
                SystemDefs.JavabaseBM.flushAllPages();
            } catch (PagePinnedException ignored) {

            } catch (Exception e) {
                System.err.println("*** error flushing pages to disk");
                e.printStackTrace();
                status = FAIL;
            }
            System.out.println();
        }
        return status;
    }

    private static boolean deleteData(String tableName, String filename) throws Exception {
        /* TBD: get schema info */

        boolean status = OK;
        Heapfile tableFile = null;
        Scan scan = null;

        if (!isTableInDB(tableName)) {
            System.err.println("*** error: relation " + tableName + " not found in DB");
            return FAIL;
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

        System.out.println("No Of atte " + attrs.length);
        for (int i = 0; i < numAttr; i++) {
            System.out.println(attrs[i].attrType);
        }
        System.out.println("\n----------------------------------");


        List<RID> recordToBeDeleted = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                /* read each line from the file, create tuple, and insert into DB */
                String[] row = line.trim().split("\\s+");
                try {
                    /* create an scan on the heapfile */
                    tableFile = new Heapfile(tableName);
                    scan = new Scan(tableFile);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    return status;
                }

                try {
                    while ((temp = scan.getNext(rid)) != null) {
                        temp = new Tuple(temp.getTupleByteArray(), temp.getOffset(), temp.getLength());
                        temp.setHdr((short) attrTypes.length, attrTypes, strSizes);
                        if (isSameTuple(row, temp, attrs)) {
                            recordToBeDeleted.add(new RID(rid.pageNo, rid.slotNo));
                        }
                    }
                    scan.closescan();
                } catch (Exception e) {
                    System.err.println(e);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        int deletionCount = 0;
        tableFile = new Heapfile(tableName);
        for (RID ridToDelete : recordToBeDeleted) {
            tableFile.deleteRecord(ridToDelete);
            deletionCount++;
        }
        System.out.println("Deletion Count is " + deletionCount);
        return OK;
    }

    private static boolean isSameTuple(String[] row, Tuple temp, AttrDesc[] attrs) {
        for (int i = 0; i < attrs.length; i++) {
            if (attrs[i].attrType.attrType == AttrType.attrInteger) {
                int val = 0;
                try {
                    val = temp.getIntFld(i + 1);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (FieldNumberOutOfBoundException e) {
                    e.printStackTrace();
                }
                if (val != Integer.parseInt(row[i])) {
                    return false;
                }
            } else if (attrs[i].attrType.attrType == AttrType.attrReal) {
                float val = 0;
                try {
                    val = temp.getFloFld(i + 1);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (FieldNumberOutOfBoundException e) {
                    e.printStackTrace();
                }
                if (val != Float.parseFloat(row[i])) {
                    return false;
                }
            } else {
                try {
                    if (!temp.getStrFld(i + 1).equals(row[i])) {
                        return false;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (FieldNumberOutOfBoundException e) {
                    e.printStackTrace();
                }
            }

        }
        return true;
    }

    private static boolean performJoin(String[] tokens) {
        String joinType = tokens[1].toLowerCase();
        String outerTableName = tokens[2].toLowerCase();
        int outerTableJoinAttr = Integer.parseInt(tokens[3]);
        String innerTableName = tokens[4].toLowerCase();
        int innerTableJoinAttr = Integer.parseInt(tokens[5]);
        int nPages = Integer.parseInt(tokens[7]);
        String outputTable = null;
        Iterator joinItr = null;

        // Currently supporting only equality
        String op = tokens[6];

        if (tokens.length > 8) {
            outputTable = tokens[9];
        }

        IteratorDesc outerItrDesc = null;
        IteratorDesc innerItrDesc = null;
        try {
            outerItrDesc = Phase3Utils.getTableItr(outerTableName);
            innerItrDesc = Phase3Utils.getTableItr(innerTableName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Condition expr
        CondExpr[] joinCond = getJoinCondition(op, outerTableJoinAttr, innerTableJoinAttr);

        int nOuterAttr = outerItrDesc.getNumAttr();
        int nInnerAttr = innerItrDesc.getNumAttr();
        int nJoinAttr = nOuterAttr + nInnerAttr;
        FldSpec[] joinProjList = new FldSpec[nJoinAttr];
        System.arraycopy(outerItrDesc.getProjlist(), 0, joinProjList, 0, nOuterAttr);
        for (int i=0; i<nInnerAttr; i++) {
            joinProjList[nOuterAttr+i] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
        }

        // Get join iterator based on joinType
        try {
            switch (joinType) {
                case "nlj": {
                    joinItr = new NestedLoopsJoins(outerItrDesc.getAttrType(), outerItrDesc.getNumAttr(), outerItrDesc.getStrSizes(),
                            innerItrDesc.getAttrType(), innerItrDesc.getNumAttr(), innerItrDesc.getStrSizes(),
                            nPages, outerItrDesc.getScan(), innerTableName, joinCond, null, joinProjList, nJoinAttr);
                    break;
                }

                case "smj": {
                    joinItr = new SortMerge(outerItrDesc.getAttrType(), outerItrDesc.getNumAttr(), outerItrDesc.getStrSizes(),
                            innerItrDesc.getAttrType(), innerItrDesc.getNumAttr(), innerItrDesc.getStrSizes(),
                            outerTableJoinAttr, STR_SIZE, innerTableJoinAttr, STR_SIZE,
                            nPages, outerItrDesc.getScan(), innerItrDesc.getScan(), false, false,
                            new TupleOrder(TupleOrder.Ascending), joinCond, joinProjList, nJoinAttr);
                    break;
                }

                case "inlj": {
                    joinItr = new IndexNestedLoopsJoins(outerItrDesc.getAttrType(), outerItrDesc.getNumAttr(), outerItrDesc.getStrSizes(),
                            innerItrDesc.getAttrType(), innerItrDesc.getNumAttr(), innerItrDesc.getStrSizes(),
                            nPages, outerItrDesc.getScan(), innerTableName, joinCond, null, joinProjList, nJoinAttr);
                    break;
                }

                case "hj": {
                    joinItr = new HashJoin(outerItrDesc.getAttrType(), outerItrDesc.getNumAttr(), outerItrDesc.getStrSizes(),
                            innerItrDesc.getAttrType(), innerItrDesc.getNumAttr(), innerItrDesc.getStrSizes(),
                            nPages, outerItrDesc.getScan(), innerTableName, joinCond, null, joinProjList, nJoinAttr);
                    break;
                }
            }

            // Print the output by performing get_next continuously
            AttrType[] jTypes = new AttrType[nJoinAttr];
            System.arraycopy(outerItrDesc.getAttrType(), 0, jTypes, 0, nOuterAttr);
            System.arraycopy(innerItrDesc.getAttrType(), 0, jTypes, nOuterAttr, nInnerAttr);

            int nOuterSizes = outerItrDesc.getStrSizes().length;
            int nInnerSizes = innerItrDesc.getStrSizes().length;
            short[] jSizes = new short[nOuterSizes + nInnerSizes];
            System.arraycopy(outerItrDesc.getStrSizes(), 0, jSizes, 0, nOuterSizes);
            System.arraycopy(innerItrDesc.getStrSizes(), 0, jSizes, nOuterSizes, nInnerSizes);


            Tuple tt = new Tuple();
            try {
                tt.setHdr((short) nJoinAttr, jTypes, jSizes);
            } catch (Exception e) {
                e.printStackTrace();
            }

            int size = tt.size();

            Tuple t = new Tuple(size);
            try {
                t.setHdr((short) nJoinAttr, jTypes, jSizes);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Heapfile hf = null;
            if (outputTable != null) {
                AttrDesc[] outerAttrs = new AttrDesc[nOuterAttr];
                ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(outerTableName, nOuterAttr, outerAttrs);
                AttrDesc[] innerAttrs = new AttrDesc[nInnerAttr];
                ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(innerTableName, nInnerAttr, innerAttrs);

                String[] fieldNames = new String[nJoinAttr];
                for (int i=0; i<nOuterAttr; i++) {
                    fieldNames[i] = outerTableName+"."+outerAttrs[i].attrName;
                    fieldNames[nOuterAttr+i] = innerTableName+"."+innerAttrs[i].attrName;
                }

                createOutputTable(outputTable, fieldNames, jTypes, nJoinAttr);

                hf = new Heapfile(outputTable);
            }

            tt = joinItr.get_next();
            int cnt = 0;
            while (tt != null) {
                t.tupleCopy(tt);
                printTuple(t, jTypes);
                if (outputTable != null) {
                    // If we want to write to table, do it
                    hf.insertRecord(t.getTupleByteArray());
                }

                cnt++;
                tt = joinItr.get_next();
            }

            System.out.println("\nNumber of joined rows: "+cnt);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    private static int getOperator(String op) {
        switch (op) {
            case ("=") : {
                return AttrOperator.aopEQ;
            }
            case ("<=") : {
                return AttrOperator.aopLE;
            }
            case ("<") : {
                return AttrOperator.aopLT;
            }
            case (">") : {
                return AttrOperator.aopGT;
            }
            case (">=") : {
                return AttrOperator.aopGE;
            }
            default: return -1;
        }
    }

    private static CondExpr[] getJoinCondition(String op, int outerAttr, int innerAttr) {
        CondExpr[] joinCond = new CondExpr[2];
        joinCond[0] = new CondExpr();
        joinCond[1] = new CondExpr();

        joinCond[0].next = null;
        assert getOperator(op) != -1;
        joinCond[0].op = new AttrOperator(getOperator(op));
        joinCond[0].type1 = new AttrType(AttrType.attrSymbol);
        joinCond[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outerAttr);
        joinCond[0].type2 = new AttrType(AttrType.attrSymbol);
        joinCond[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), innerAttr);

        joinCond[1] = null;

        return joinCond;
    }

    private static void createOutputTable(String outputTable, String[] fieldNames, AttrType[] jtypes, int nJoinAttr) throws Exception {
        try {
            int SIZE_OF_INT = 4;
            attrInfo[] attrs = new attrInfo[nJoinAttr];

            for (int i = 0; i < nJoinAttr; ++i) {
                attrs[i] = new attrInfo();
                attrs[i].attrType = new AttrType(jtypes[i].attrType);
                attrs[i].attrName = fieldNames[i];
                attrs[i].attrLen = (jtypes[i].attrType == AttrType.attrInteger) ? SIZE_OF_INT : STR_SIZE;
            }
            ExtendedSystemDefs.MINIBASE_RELCAT.createRel(outputTable, nJoinAttr, attrs);
        } catch (Exception e) {
            throw new Exception("Create output table failed: ", e);
        }
    }

    public static void main(String[] args) {

        try {
            dbShell();
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}