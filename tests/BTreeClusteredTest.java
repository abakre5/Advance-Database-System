package tests;


import btree.*;
import diskmgr.FileIOException;
import diskmgr.Page;
import global.*;
import heap.*;
import index.*;
import iterator.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

class BTreeClusteredDriver extends TestDriver implements GlobalConst {
    private int TRUE = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private List playersList;

    /**
     * BTreeClusteredDriver Constructor, inherited from TestDriver
     */
    public BTreeClusteredDriver() {
        super("BTreeClusteredTest");
    }

    /**
     * calls the runTests function in TestDriver
     */
    public boolean runTests() {

        System.out.print("\n" + "Running " + testName() + " tests...." + "\n");

        try {
            SystemDefs sysdef = new SystemDefs(dbpath, NUMBUF + 10000, NUMBUF + 10000, "Clock");
        } catch (Exception e) {
            Runtime.getRuntime().exit(1);
        }

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

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

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);

        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    protected boolean runAllTests() {

        boolean _passAll = OK;

        //The following runs all the test functions

        //Running test1() to test6()
        if (!test3()) {
            _passAll = FAIL;
        }

        return _passAll;
    }

    public boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");
        boolean status = OK;
        int SORTPGNUM = 100;

        AttrType[] Ptypes = new AttrType[5];
        Ptypes[0] = new AttrType (AttrType.attrReal);
        Ptypes[1] = new AttrType (AttrType.attrReal);
        Ptypes[2] = new AttrType (AttrType.attrReal);
        Ptypes[3] = new AttrType (AttrType.attrReal);
        Ptypes[4] = new AttrType (AttrType.attrReal);

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN2;
        attrSize[1] = REC_LEN1;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5,Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        Heapfile tempFile = null;
        String tempFileName = "unsorted_data.in";
        String heapFile = "BTreeClustered.in";
        String tempfile = "temp";
        String newIndex = "BTreeClusteredIndex_rev";
        String indexFile  = "BTreeClusteredIndex" ;
        try {
            f = new Heapfile(heapFile);
            tempFile = new Heapfile(tempFileName);
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        try (BufferedReader br = new BufferedReader(new FileReader("data2.txt")))
        {
            String line;
            boolean flag = false;
            while ((line = br.readLine()) != null) {
                if (flag) {
                    String[] ans = line.split("\\s+");
                    for (int i = 1;i <= ans.length;i++) {
                        t.setFloFld(i, Float.parseFloat(ans[i - 1]));
                    }
                    try {
                        rid = tempFile.insertRecord(t.returnTupleByteArray());
                    }
                    catch (Exception e) {
                        System.err.println("*** error in Heapfile.insertRecord() ***");
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }
                }
                flag = true;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        }


        FldSpec[] Pprojection = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        Pprojection[0] = new FldSpec(rel, 1);
        Pprojection[1] = new FldSpec(rel, 2);
        Pprojection[2] = new FldSpec(rel, 3);
        Pprojection[3] = new FldSpec(rel, 4);
        Pprojection[4] = new FldSpec(rel, 5);
        // Scan the players table
        FileScan am = null;
        try {
            am  = new FileScan(tempFileName, Ptypes, attrSize,
                    (short)5, (short)5,
                    Pprojection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for players");
            Runtime.getRuntime().exit(1);
        }


        Sort sort = null;
        try {
            sort = new Sort(Ptypes, (short) Ptypes.length, attrSize, am, 1, order[1], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = null;
        try{
            t = sort.get_next();
        } catch(Exception e){
            status = FAIL;
            e.printStackTrace();
        }

        while (t!= null)
        {
            try
            {
                //System.out.println(" tuple field "+ t.getFloFld(1));
                f.insertRecord(t.getTupleByteArray());
                t= sort.get_next();
            } catch(Exception e)
            {
                status = FAIL;
                e.printStackTrace();
            }

            if(status == FAIL)
            {
                break;
            }
        }

        try
        {
            sort.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        int[] pref_list = new int[1];
        pref_list[0] = 1;

        // create the index file
        BTreeClusteredFile btf = null;
        try
        {
            btf = new BTreeClusteredFile(indexFile, AttrType.attrReal, REC_LEN1, 1/*delete*/);
        } catch (Exception e)
        {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        rid = new RID();
        Float key = null;

        Tuple temp = null;
        int prev = -1;

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        //prev = rid.pageNo.pid;
        Tuple prevTuple = null;
        while (temp != null)
        {
            if(temp != null )
            {
                t.tupleCopy(temp);
            }

            // insert first key from each page in index
            if(prev != rid.pageNo.pid)
            {
                prev = rid.pageNo.pid;
                if(t != null)
                {
                    try
                    {
                        key = t.getFloFld(pref_list[0]) * -1;
                    } catch (Exception e)
                    {
                        status = FAIL;
                        e.printStackTrace();
                    }
                    System.out.println("page no " + rid.pageNo.pid + " slot no " + rid.slotNo);

                    try {

                        btf.insert(new FloatKey(key), new PageId(prev));
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }


                }
            }

            try
            {
                temp = scan.getNext(rid);
            }
            catch (Exception e)
            {
                status = FAIL;
                e.printStackTrace();
            }

            if(status == FAIL)
            {
                break;
            }
        }

        System.out.println("BTree Clustered Index created successfully.\n");

        try
        {
           BT.printBTree(btf.getHeaderPage());
           BT.printAllLeafPages(btf.getHeaderPage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("----------Printing data using index scan on clustered btree index----------.\n");
        IndexScan indexScan = null;
        try {
           indexScan = new IndexScan(new IndexType(IndexType.B_ClusteredIndex), heapFile, indexFile, Ptypes, attrSize, 5,5, Pprojection,null,1,false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Tuple indexTuple = null;

        try {
            indexTuple = indexScan.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }


        try
        {
            while(indexTuple != null)
            {
                t = new Tuple(indexTuple.getTupleByteArray(), indexTuple.getOffset(), indexTuple.getLength());
                t.setHdr((short) 5, Ptypes, attrSize);
                //System.out.println(t.getFloFld(1) + "\t" + t.getFloFld(2) + "\t" +t.getFloFld(3) + "\t"+t.getFloFld(4) + "\t"+t.getFloFld(5));
                indexTuple = indexScan.get_next();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


        try
        {
            tempFile.deleteFile();
            f.deleteFile();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        System.out.println("------------------- TEST 1 completed ---------------------\n");
        return status;
    }

    public boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");
        boolean status = OK;
        int SORTPGNUM = 100;

        AttrType[] Ptypes = new AttrType[5];
        Ptypes[0] = new AttrType (AttrType.attrReal);
        Ptypes[1] = new AttrType (AttrType.attrReal);
        Ptypes[2] = new AttrType (AttrType.attrReal);
        Ptypes[3] = new AttrType (AttrType.attrReal);
        Ptypes[4] = new AttrType (AttrType.attrReal);

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN2;
        attrSize[1] = REC_LEN1;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5,Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        Heapfile tempFile = null;
        Heapfile newDataFile = null;
        String tempFileName = "unsorted_data.in";
        String heapFile = "BTreeClustered.in";
        String newheapFile = "btc_rev_";
        String newDataFileName = "temp";
        String newIndex = "BTreeClusteredIndex_rev";
        String indexFile  = "BTreeClusteredIndex" ;
        try {
            f = new Heapfile(heapFile);
            tempFile = new Heapfile(tempFileName);
            newDataFile = new Heapfile(newDataFileName);
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        try (BufferedReader br = new BufferedReader(new FileReader("data_all.txt")))
        {
            String line;
            boolean flag = false;
            while ((line = br.readLine()) != null) {
                if (flag) {
                    String[] ans = line.split("\\s+");
                    for (int i = 1;i <= ans.length;i++) {
                        t.setFloFld(i, Float.parseFloat(ans[i - 1]));
                    }
                    try {
                        rid = tempFile.insertRecord(t.returnTupleByteArray());
                    }
                    catch (Exception e) {
                        System.err.println("*** error in Heapfile.insertRecord() ***");
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }
                }
                flag = true;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        }

        try (BufferedReader br = new BufferedReader(new FileReader("data_new.txt")))
        {
            String line;
            boolean flag = false;
            while ((line = br.readLine()) != null) {
                if (flag) {
                    String[] ans = line.split("\\s+");
                    for (int i = 1;i <= ans.length;i++) {
                        t.setFloFld(i, Float.parseFloat(ans[i - 1]));
                    }
                    try {
                        rid = newDataFile.insertRecord(t.returnTupleByteArray());
                    }
                    catch (Exception e) {
                        System.err.println("*** error in Heapfile.insertRecord() ***");
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }
                }
                flag = true;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        }


        FldSpec[] Pprojection = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        Pprojection[0] = new FldSpec(rel, 1);
        Pprojection[1] = new FldSpec(rel, 2);
        Pprojection[2] = new FldSpec(rel, 3);
        Pprojection[3] = new FldSpec(rel, 4);
        Pprojection[4] = new FldSpec(rel, 5);
        // Scan the players table
        FileScan am = null;
        try {
            am  = new FileScan(tempFileName, Ptypes, attrSize,
                    (short)5, (short)5,
                    Pprojection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for players");
            Runtime.getRuntime().exit(1);
        }


        Sort sort = null;
        try {
            sort = new Sort(Ptypes, (short) Ptypes.length, attrSize, am, 1, order[1], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = null;
        try{
            t = sort.get_next();
        } catch(Exception e){
            status = FAIL;
            e.printStackTrace();
        }

        while (t!= null)
        {
            try
            {
                //System.out.println(" tuple field "+ t.getFloFld(1));
                f.insertRecord(t.getTupleByteArray());
                t= sort.get_next();
            } catch(Exception e)
            {
                status = FAIL;
                e.printStackTrace();
            }

            if(status == FAIL)
            {
                break;
            }
        }

        try
        {
            sort.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        int[] pref_list = new int[1];
        pref_list[0] = 1;

        // create the index file
        BTreeClusteredFile btf = null;
        try
        {
            btf = new BTreeClusteredFile(indexFile, AttrType.attrReal, REC_LEN1, 1/*delete*/);
        } catch (Exception e)
        {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        rid = new RID();
        Float key = null;

        Tuple temp = null;
        int prev = -1;

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        //prev = rid.pageNo.pid;
        Tuple prevTuple = null;
        while (temp != null)
        {
            if(temp != null )
            {
                t.tupleCopy(temp);
            }

            // insert first key from each page in index
            if(prev != rid.pageNo.pid)
            {
                prev = rid.pageNo.pid;
                if(t != null)
                {
                    try
                    {
                        key = t.getFloFld(pref_list[0]) * -1;
                    } catch (Exception e)
                    {
                        status = FAIL;
                        e.printStackTrace();
                    }
                    System.out.println("page no " + rid.pageNo.pid + " slot no " + rid.slotNo);

                    try {

                        btf.insert(new FloatKey(key), new PageId(prev));
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
            }

            try
            {
                temp = scan.getNext(rid);
            }
            catch (Exception e)
            {
                status = FAIL;
                e.printStackTrace();
            }

            if(status == FAIL)
            {
                break;
            }
        }

        System.out.println("BTree Clustered Index created successfully.\n");

        try
        {
            BT.printBTree(btf.getHeaderPage());
            BT.printAllLeafPages(btf.getHeaderPage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("----------Printing data using index scan on clustered btree index----------.\n");
        IndexScan indexScan = null;
        try {
            indexScan = new IndexScan(new IndexType(IndexType.B_ClusteredIndex), heapFile, indexFile, Ptypes, attrSize, 5,5, Pprojection,null,1,false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Tuple indexTuple = null;

        try {
            indexTuple = indexScan.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }


        try
        {
            while(indexTuple != null)
            {
                t = new Tuple(indexTuple.getTupleByteArray(), indexTuple.getOffset(), indexTuple.getLength());
                t.setHdr((short) 5, Ptypes, attrSize);
                //System.out.println(t.getFloFld(1) + "\t" + t.getFloFld(2) + "\t" +t.getFloFld(3) + "\t"+t.getFloFld(4) + "\t"+t.getFloFld(5));
                indexTuple = indexScan.get_next();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            //Runtime.getRuntime().exit(1);
        }
        System.out.println("----------End printing----------.\n");

        System.out.println("Inserting new data\n");


        FileScan am2 = null;
        try {
            am2  = new FileScan(newDataFileName, Ptypes, attrSize,
                    (short)5, (short)5,
                    Pprojection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error while scanning new data file");
            //Runtime.getRuntime().exit(1);
        }


        Sort sortIterator = null;
        try {
            sortIterator = new Sort(Ptypes, (short) Ptypes.length, attrSize, am2, 1, order[1], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            mergeSortedHeapFiles(heapFile, sortIterator, newheapFile, 1, Ptypes, attrSize);
        } catch (Exception e) {
            e.printStackTrace();
        }

        FileScan am3 = null;
        try {
            am3  = new FileScan(newheapFile, Ptypes, attrSize,
                    (short)5, (short)5,
                    Pprojection, null);
            temp = am3.get_next();

            while(temp != null){
                t = new Tuple(temp.getTupleByteArray(), temp.getOffset(), temp.getLength());
                t.setHdr((short) 5, Ptypes, attrSize);
                System.out.println(t.getFloFld(1) + "\t" + t.getFloFld(2) + "\t" +t.getFloFld(3) + "\t"+t.getFloFld(4) + "\t"+t.getFloFld(5));
                temp = am3.get_next();
            }

        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error while scanning new data file");
            Runtime.getRuntime().exit(1);
        }





//        try
//        {
//            tempFile.deleteFile();
//            f.deleteFile();
//        } catch (Exception e)
//        {
//            e.printStackTrace();
//        }

        System.out.println("------------------- TEST 2 completed ---------------------\n");
        return status;
    }

    public boolean test3() {
        System.out.println("------------------------ TEST 3 --------------------------");
        boolean status = OK;
        int SORTPGNUM = 100;

        AttrType[] Ptypes = new AttrType[5];
        Ptypes[0] = new AttrType (AttrType.attrReal);
        Ptypes[1] = new AttrType (AttrType.attrReal);
        Ptypes[2] = new AttrType (AttrType.attrReal);
        Ptypes[3] = new AttrType (AttrType.attrReal);
        Ptypes[4] = new AttrType (AttrType.attrReal);

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN2;
        attrSize[1] = REC_LEN1;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5,Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        Heapfile tempFile = null;
        String tempFileName = "unsorted_data.in";
        String heapFile = "BTreeClustered.in";
        String tempfile = "temp";
        String newIndex = "BTreeClusteredIndex_rev";
        String indexFile  = "BTreeClusteredIndex" ;
        try {
            f = new Heapfile(heapFile);
            f.setPageUtilization(1f);
            tempFile = new Heapfile(tempFileName);
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        try (BufferedReader br = new BufferedReader(new FileReader("data_all.txt")))
        {
            String line;
            boolean flag = false;
            while ((line = br.readLine()) != null) {
                if (flag) {
                    String[] ans = line.split("\\s+");
                    for (int i = 1;i <= ans.length;i++) {
                        t.setFloFld(i, Float.parseFloat(ans[i - 1]));
                    }
                    try {
                        rid = tempFile.insertRecord(t.returnTupleByteArray());
                    }
                    catch (Exception e) {
                        System.err.println("*** error in Heapfile.insertRecord() ***");
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }
                }
                flag = true;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        }


        FldSpec[] Pprojection = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        Pprojection[0] = new FldSpec(rel, 1);
        Pprojection[1] = new FldSpec(rel, 2);
        Pprojection[2] = new FldSpec(rel, 3);
        Pprojection[3] = new FldSpec(rel, 4);
        Pprojection[4] = new FldSpec(rel, 5);
        // Scan the players table
        FileScan am = null;
        try {
            am  = new FileScan(tempFileName, Ptypes, attrSize,
                    (short)5, (short)5,
                    Pprojection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for players");
            Runtime.getRuntime().exit(1);
        }


        Sort sort = null;
        try {
            sort = new Sort(Ptypes, (short) Ptypes.length, attrSize, am, 1, order[0], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = null;
        try{
            t = sort.get_next();
        } catch(Exception e){
            status = FAIL;
            e.printStackTrace();
        }

        while (t!= null)
        {
            try
            {
                //System.out.println(" tuple field "+ t.getFloFld(1));
                f.insertRecord(t.getTupleByteArray());
                t= sort.get_next();
            } catch(Exception e)
            {
                status = FAIL;
                e.printStackTrace();
            }

            if(status == FAIL)
            {
                break;
            }
        }

        try
        {
            sort.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        int[] pref_list = new int[1];
        pref_list[0] = 1;

        // create the index file
        BTreeClusteredFile btf = null;
        try
        {
            btf = new BTreeClusteredFile(indexFile, AttrType.attrReal, REC_LEN1, 1/*delete*/, heapFile,Ptypes, attrSize);
        } catch (Exception e)
        {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


        rid = new RID();
        Float key = null;

        Tuple temp = null;
        int prev = -1;

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }



        //prev = rid.pageNo.pid;
        Tuple prevTuple = null;
        while (temp != null)
        {
            if(temp != null )
            {
                t.tupleCopy(temp);
            }

            // insert first key from each page in index
            if(prev != rid.pageNo.pid)
            {
                prev = rid.pageNo.pid;
                if(t != null)
                {
                    try
                    {
                        key = t.getFloFld(pref_list[0]) * 1;
                    } catch (Exception e)
                    {
                        status = FAIL;
                        e.printStackTrace();
                    }
                    //System.out.println("page no " + rid.pageNo.pid + " slot no " + rid.slotNo);

                    try {

                        btf.insert(new FloatKey(key), new PageId(prev));
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }


                }
            }


            try
            {
                temp = scan.getNext(rid);
            }
            catch (Exception e)
            {
                status = FAIL;
                e.printStackTrace();
            }

            if(status == FAIL)
            {
                break;
            }
        }

        btf.SetHeapFile(f);

        System.out.println("BTree Clustered Index created successfully.\n");

        try
        {
            //BT.printBTree(btf.getHeaderPage());
            //BT.printAllLeafPages(btf.getHeaderPage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("----------Printing data using index scan on clustered btree index----------.\n");
        IndexScan indexScan = null;
        try {
            indexScan = new IndexScan(new IndexType(IndexType.B_ClusteredIndex), heapFile, indexFile, Ptypes, attrSize, 5,5, Pprojection,null,1,false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Tuple indexTuple = null;

        try {
            indexTuple = indexScan.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }


        try
        {
            while(indexTuple != null)
            {
                t = new Tuple(indexTuple.getTupleByteArray(), indexTuple.getOffset(), indexTuple.getLength());
                t.setHdr((short) 5, Ptypes, attrSize);
                //System.out.println(t.getFloFld(1) + "\t" + t.getFloFld(2) + "\t" +t.getFloFld(3) + "\t"+t.getFloFld(4) + "\t"+t.getFloFld(5));
                indexTuple = indexScan.get_next();
            }

            int count = 50;

            t.setFloFld(1, 0.8f);
            t.setFloFld(2, 0.8f);
            t.setFloFld(3, 0.8f);
            t.setFloFld(4, 0.8f);
            t.setFloFld(5, 0.8f);
            float val  = .8f;

            while(count > 0){
                //val += 0.1f;
                t.setFloFld(1, val);
                t.setFloFld(2, val);
                t.setFloFld(3, val);
                t.setFloFld(4, val);
                t.setFloFld(5, val);
                KeyClass key1 = BT.createKeyFromTupleField(t, Ptypes, 1, 1);
                rid =  btf.insertTuple(t, key1, 1, 1);
                System.out.println("RID : " + rid.pageNo.pid + " " + rid.slotNo);
                count--;
            }

        } catch (Exception e)
        {
            e.printStackTrace();
           // Runtime.getRuntime().exit(1);
        }

        try
        {
            //BT.printBTree(btf.getHeaderPage());
            BT.printAllLeafPages(btf.getHeaderPage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        IndexScan indexScan2 = null;
        try {
            indexScan2 = new IndexScan(new IndexType(IndexType.B_ClusteredIndex), heapFile, indexFile, Ptypes, attrSize, 5,5, Pprojection,null,1,false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        indexTuple = null;

        try {
            indexTuple = indexScan2.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }


        try
        {
            while(indexTuple != null)
            {
                t = new Tuple(indexTuple.getTupleByteArray(), indexTuple.getOffset(), indexTuple.getLength());
                t.setHdr((short) 5, Ptypes, attrSize);
                System.out.println(t.getFloFld(1) + "\t" + t.getFloFld(2) + "\t" +t.getFloFld(3) + "\t"+t.getFloFld(4) + "\t"+t.getFloFld(5));
                indexTuple = indexScan2.get_next();
            }

        } catch (Exception e)
        {
            e.printStackTrace();
            //Runtime.getRuntime().exit(1);
        }

        Scan heapscan = null;
        try {
            heapscan = new Scan(f);
        } catch (Exception e) {
            e.printStackTrace();
        }

        indexTuple = null;
        rid = new RID();

        try {
            indexTuple = heapscan.getNext(rid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("-----------------Deleting heap file----------------");
        try
        {
            boolean b  = btf.deleteTuple(new RID(new PageId(117), 0));
            if(!b){
                System.out.println("Failed to delete record ");
            }

             b  = btf.deleteTuple(new RID(new PageId(118), 0));
            if(!b){
                System.out.println("Failed to delete record ");
            }

            b  = btf.deleteTuple(new RID(new PageId(120), 0));
            if(!b){
                System.out.println("Failed to delete record ");
            }

            ArrayList<RID> rids = new ArrayList<>();
            while(indexTuple != null)
            {
                t = new Tuple(indexTuple.getTupleByteArray(), indexTuple.getOffset(), indexTuple.getLength());
                t.setHdr((short) 5, Ptypes, attrSize);
                String rd = "RID : "  + rid.pageNo.pid + ":"+ rid.slotNo +" ";
               // System.out.println( rd +  t.getFloFld(1) + "\t" + t.getFloFld(2) + "\t" +t.getFloFld(3) + "\t"+t.getFloFld(4) + "\t"+t.getFloFld(5));
                indexTuple = heapscan.getNext(rid);
            }
            heapscan.closescan();

        } catch (Exception e)
        {
            e.printStackTrace();
            //Runtime.getRuntime().exit(1);
        }

        try
        {
            //BT.printBTree(btf.getHeaderPage());
            BT.printAllLeafPages(btf.getHeaderPage());
        } catch (Exception e) {
            e.printStackTrace();
        }

//        System.out.println("----------Searching clustered btree----------.\n");
//        try
//        {
//           BTClusteredFileScan btClusteredFileScan = btf.new_scan(new FloatKey(0.8f), null);
//           KeyDataEntry entry  = btClusteredFileScan.get_next();
//           while (entry != null){
//               PageId pagedId = ((ClusteredLeafData)entry.data).getData();
//               System.out.println("Page no having records in range : " + pagedId);
//               entry = btClusteredFileScan.get_next();
//           }
//        } catch (Exception e)
//        {
//            e.printStackTrace();
//        }

//        try
//        {
//            tempFile.deleteFile();
//            f.deleteFile();
//        } catch (Exception e)
//        {
//            e.printStackTrace();
//        }

        System.out.println("------------------- TEST 3 completed ---------------------\n");
        return status;
    }

    /**
     * overrides the testName function in TestDriver
     *
     * @return the name of the test
     */
    protected String testName() {
        return "BTreeClustered";
    }

    private boolean CreateClusteredIndex(Heapfile f, BTreeClusteredFile btf, AttrType[] attrTypes, short[] attrSize, int fieldNumber) throws InvalidTupleSizeException, IOException, InvalidTypeException {
        boolean status = OK;
        RID rid = new RID();
        KeyClass key = null;
        Tuple temp = null;
        int prev = -1;

        Scan scan = null;
        scan = new Scan(f);

        Tuple t = null;

        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        while (temp != null)
        {
            if(temp != null )
            {
                t = new Tuple(temp.getTupleByteArray(), temp.getOffset(), temp.getLength());
                t.setHdr((short) attrTypes.length, attrTypes, attrSize);
            }

            // insert first key from each page in index
            if(prev != rid.pageNo.pid)
            {
                prev = rid.pageNo.pid;
                if(t != null)
                {
                    try
                    {
                        key = BT.createKeyFromTupleField(t, attrTypes, 1, -1);
                    } catch (Exception e)
                    {
                        status = FAIL;
                        e.printStackTrace();
                    }
                    System.out.println("page no " + rid.pageNo.pid + " slot no " + rid.slotNo);

                    try {
                        btf.insert(key, new PageId(prev));
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
            }

            try
            {
                temp = scan.getNext(rid);
            }
            catch (Exception e)
            {
                status = FAIL;
                e.printStackTrace();
            }

            if(status == FAIL)
            {
                break;
            }
        }
        return status;
    }

    private void mergeSortedHeapFiles(String dataFile, Sort sortIterator, String mergedFile,
                                      int sortAttr, AttrType[] attrTypes,
                                      short[] attrSize)
            throws InvalidTupleSizeException, LowMemException, SortException, JoinsException,
            UnknowAttrType, IOException
    {
        Heapfile hfDataFile = null;
        Heapfile hfMergedFile = null;
        try {
          //   hfDataFile = Phase3Utils.getHeapFileInstance(dataFile);
            // hfMergedFile = Phase3Utils.getHeapFileInstance(mergedFile);
        } catch (Exception e) {
            e.printStackTrace();
        }


        Tuple t1 = null, t2 = null;
        Tuple t1Copy = null, t2Copy = null;
        RID rid = new RID();


        Scan dataFileScanner = null;
        try{
            dataFileScanner =  new Scan(hfDataFile);

            t1 = dataFileScanner.getNext(rid);
            t2 = sortIterator.get_next();

            if(t2 == null || t1 == null){
                System.out.println("initial tuples are null");
                return;
            }

        }catch(Exception e){
            e.printStackTrace();
        }


        try{
             while(t1 != null && t2 != null)
             {
                 t1Copy = new Tuple(t1.getTupleByteArray(), t1.getOffset(), t1.getLength());
                 t1Copy.setHdr((short) 5, attrTypes, attrSize);
                 t2Copy = new Tuple(t2.getTupleByteArray(), t2.getOffset(), t2.getLength());
                 t2Copy.setHdr((short) 5, attrTypes, attrSize);
                 if(TupleUtils.CompareTupleWithTuple(attrTypes[sortAttr],t1Copy, sortAttr, t2Copy, sortAttr) >= 0)
                 {
                     //t1 is smaller, insert it first
                     hfMergedFile.insertRecord(t1.getTupleByteArray());
                     t1 = dataFileScanner.getNext(rid);
                 }
                 else
                 {
                     hfMergedFile.insertRecord(t2.getTupleByteArray());
                     t2 = sortIterator.get_next();
                 }
             }

             while (t1 != null)
             {
                 hfMergedFile.insertRecord(t1.getTupleByteArray());
                 t1 = dataFileScanner.getNext(rid);
             }

             while(t2!= null){
                 hfMergedFile.insertRecord(t2.getTupleByteArray());
                 t2 = sortIterator.get_next();
             }

        }catch(Exception e)
        {
            e.printStackTrace();
        }

    }
}

public class BTreeClusteredTest {
    public static void main(String[] args) {

        BTreeClusteredDriver nlsDriver = new BTreeClusteredDriver();
        boolean dbstatus;

        dbstatus = nlsDriver.runTests();

        if (!dbstatus) {
            System.out.println("Error encountered during BTreeClustered tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}
