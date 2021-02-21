package tests;

import global.*;
import heap.Tuple;
import iterator.*;

import java.io.IOException;

/**
 * Created on 02/19/2021
 * @Author Abhishek and Manthan
 */
class CompareTuplePrefDriver extends TestDriver implements GlobalConst {
    private int TRUE = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;

    /**
     * CompareTuplePrefDriver Constructor, inherited from TestDriver
     */
    public CompareTuplePrefDriver() {
        super("CompareTuplePrefTest");
    }

    /**
     * calls the runTests function in TestDriver
     */
    public boolean runTests() {

        System.out.print("\n" + "Running " + testName() + " tests...." + "\n");

        try {
            SystemDefs sysdef = new SystemDefs(dbpath, NUMBUF + 20, NUMBUF, "Clock");
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
        if (!test2()) {
            _passAll = FAIL;
        }
        if (!test3()) {
            _passAll = FAIL;
        }
        if (!test4()) {
            _passAll = FAIL;
        }
        if (!test5()) {
            _passAll = FAIL;
        }
        if (!test6()) {
            _passAll = FAIL;
        }

        return _passAll;
    }

    /**
     * TEST Compare Tuple Pref
     *
     * @return
     */
    public boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");
        boolean status = OK;

        int[] prefList = new int[] {1, 2};

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[2];

        // create a tuple of appropriate size
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();
        try {
            t1.setHdr((short) 2, attrType, attrSize);
            t2.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        try {
            t1.setIntFld(1, 1);
            t1.setFloFld(2, 1.1f);
            t2.setIntFld(1, 2);
            t2.setFloFld(2, 1.1f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            int comp = TupleUtils.CompareTupleWithTuplePref(t1, attrType, t2, attrType, (short) 2, attrSize, prefList, prefList.length);
            System.out.println("T1 compares T2: " + comp);
            status = OK;
        } catch (Exception  e) {
            System.out.println("Error occurred while executing compare tuple pref => " +  e);
            e.printStackTrace();
            status = FAIL;
        }

        System.out.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }

    /**
     * TEST Compare Tuple Pref
     *
     * @return
     */
    public boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");
        boolean status = OK;

        int[] prefList = new int[] {1, 2};

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[2];

        // create a tuple of appropriate size
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();
        try {
            t1.setHdr((short) 2, attrType, attrSize);
            t2.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        try {
            t1.setIntFld(1, 1);
            t1.setFloFld(2, 1.1f);
            t2.setIntFld(1, 1);
            t2.setFloFld(2, 1.1f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            int comp = TupleUtils.CompareTupleWithTuplePref(t1, attrType, t2, attrType, (short) 2, attrSize, prefList, prefList.length);
            System.out.println("T1 compares T2: " + comp);
            status = OK;
        } catch (Exception  e) {
            System.out.println("Error occurred while executing compare tuple pref => " +  e);
            e.printStackTrace();
            status = FAIL;
        }

        System.out.println("------------------- TEST 2 completed ---------------------\n");

        return status;
    }

    /**
     * TEST Compare Tuple Pref
     *
     * @return
     */
    public boolean test3() {
        System.out.println("------------------------ TEST 3 --------------------------");
        boolean status = OK;

        int[] prefList = new int[] {1, 2};

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[2];

        // create a tuple of appropriate size
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();
        try {
            t1.setHdr((short) 2, attrType, attrSize);
            t2.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        try {
            t1.setIntFld(1, 2);
            t1.setFloFld(2, 1.1f);
            t2.setIntFld(1, 1);
            t2.setFloFld(2, 1.1f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            int comp = TupleUtils.CompareTupleWithTuplePref(t1, attrType, t2, attrType, (short) 2, attrSize, prefList, prefList.length);
            System.out.println("T1 compares T2: " + comp);
            status = OK;
        } catch (Exception  e) {
            System.out.println("Error occurred while executing compare tuple pref => " +  e);
            e.printStackTrace();
            status = FAIL;
        }

        System.out.println("------------------- TEST 3 completed ---------------------\n");

        return status;
    }

    /**
     * TEST Compare Tuple Pref
     *
     * @return
     */
    public boolean test4() {
        System.out.println("------------------------ TEST 4 --------------------------");
        boolean status = OK;

        int[] prefList = new int[] {1, 2};

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[] {5};

        // create a tuple of appropriate size
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();
        try {
            t1.setHdr((short) 2, attrType, attrSize);
            t2.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        try {
            t1.setStrFld(1, "Hello");
            t1.setFloFld(2, 1.1f);
            t2.setStrFld(1, "Holla");
            t2.setFloFld(2, 1.1f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            int comp = TupleUtils.CompareTupleWithTuplePref(t1, attrType, t2, attrType, (short) 2, attrSize, prefList, prefList.length);
            status = FAIL;
        } catch (Exception  e) {
            System.out.println("String operator not supported => " + e);
            System.out.println("Test passed successfully.");
            status = OK;
        }

        System.out.println("------------------- TEST 4 completed ---------------------\n");

        return status;
    }

    /**
     * overrides the testName function in TestDriver
     *
     * @return the name of the test
     */
    protected String testName() {
        return "Compare Tuple Pref(SUM)";
    }
}

public class CompareTuplePrefTest {

    public static void main(String[] args) {

        CompareTuplePrefDriver compareTuplePrefDriver = new CompareTuplePrefDriver();
        boolean dbstatus;

        dbstatus = compareTuplePrefDriver.runTests();

        if (!dbstatus) {
            System.out.println("Error encountered during Compare Tuple Pref tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}
