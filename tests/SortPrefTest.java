package tests;

import java.io.*;

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;

import java.util.Random;


class SORTPrefDriver extends TestDriver implements GlobalConst {

    private static int data1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static float data2[] = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 6.6f, 7.7f, 8.8f, 9.9f};
    private static int data3[] = {9, 8, 7, 6, 5, 4, 3, 2, 1};
    private static float data4[] = {9.9f, 8.8f, 7.7f, 6.6f, 5.5f, 4.4f, 3.3f, 2.2f, 1.1f};


    private static int NUM_RECORDS = data2.length;
    private static int LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private static int SORTPGNUM = 12;


    public SORTPrefDriver() {
        super("sortPreftest");
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 300, NUMBUF, "Clock");

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
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
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

        System.out.println("\n" + "..." + testName() + " tests ");
        System.out.println(_pass == OK ? "completely successfully" : "failed");
        System.out.println(".\n\n");

        return _pass;
    }

    protected boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[3];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[1];
        attrSize[0] = REC_LEN1;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("test1.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < NUM_RECORDS; i++) {
            try {
                t.setIntFld(1, data1[i]);
                t.setFloFld(2, data2[i]);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1.in"
        /*Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 2, attrSize, fscan, 1, order[0], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }*/
        int[] pref_list = new int[]{1, 2};
        SortPref sort = null;
        try {
            sort = new SortPref(attrType, (short) 2, attrSize, fscan, order[1], pref_list, pref_list.length, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
        int outval1 = 0;
        float outval2 = 0.0f;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        while (t != null) {
            if (count >= NUM_RECORDS) {
                System.err.println("Test1 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval1 = t.getIntFld(1);
                outval2 = t.getFloFld(2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!(outval1 == data3[count] && outval2 == data4[count])) {
                System.out.println();
                System.err.println("outval = " + outval1 + "\tdata2[count] = " + data2[count]);

                System.err.println("Test1 -- OOPS! test1.out not sorted");
                status = FAIL;
            }
            count++;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < NUM_RECORDS) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test1 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.err.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }


    protected boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[3];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[1];
        attrSize[0] = REC_LEN1;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("test2.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < NUM_RECORDS; i++) {
            try {
                t.setIntFld(1, data1[i]);
                t.setFloFld(2, data2[i]);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test2.in", attrType, attrSize, (short) 2, 2, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1.in"
        /*Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 2, attrSize, fscan, 1, order[0], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }*/
        int[] pref_list = new int[]{1, 2};
        SortPref sort = null;
        try {
            sort = new SortPref(attrType, (short) 2, attrSize, fscan, order[0], pref_list, pref_list.length, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
        int outval1 = 0;
        float outval2 = 0.0f;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        while (t != null) {
            if (count >= NUM_RECORDS) {
                System.err.println("Test2 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval1 = t.getIntFld(1);
                outval2 = t.getFloFld(2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!(outval1 == data1[count] && outval2 == data2[count])) {
                System.out.println();
                System.err.println("outval = " + outval1 + "\tdata2[count] = " + data2[count]);

                System.err.println("Test2 -- OOPS! test1.out not sorted");
                status = FAIL;
            }
            count++;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < NUM_RECORDS) {
            System.err.println("Test2 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test2 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.err.println("------------------- TEST 2 completed ---------------------\n");

        return status;
    }

    protected String testName() {
        return "SortPref";
    }
}

public class SortPrefTest {
    public static void main(String argv[]) {
        boolean sortstatus;

        SORTPrefDriver sortt = new SORTPrefDriver();

        sortstatus = sortt.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        } else {
            System.out.println("Sorting tests completed successfully");
        }
    }
}

