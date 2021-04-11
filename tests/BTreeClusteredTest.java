package tests;


import btree.*;
import global.*;
import heap.*;
import index.IndexScan;
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
        if (!test1()) {
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

        try (BufferedReader br = new BufferedReader(new FileReader("data3.txt")))
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
            btf = new BTreeClusteredFile("BTreeClusteredIndex", AttrType.attrReal, REC_LEN1, 1/*delete*/);
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
            //System.out.println(" page no " + rid.pageNo.pid);
            // insert first key from each page in index
            if(prev != rid.pageNo.pid)
            {
                prev = rid.pageNo.pid;
                if(t != null)
                {
                    try
                    {
                        key = t.getFloFld(pref_list[0]);
                    } catch (Exception e)
                    {
                        status = FAIL;
                        e.printStackTrace();
                    }

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

    /**
     * overrides the testName function in TestDriver
     *
     * @return the name of the test
     */
    protected String testName() {
        return "BTreeClustered";
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
