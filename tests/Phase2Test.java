package tests;

import java.io.*;
import java.util.*;
import java.lang.*;

import chainexception.ChainException;
import diskmgr.Page;
import diskmgr.PageCounter;
import global.*;
import heap.*;
import iterator.*;

class Args {
    String  skylineMethod;
    String  datafile;
    int[]   pref_attribs;
    int     n_pages;
}

class Ph2Driver extends TestDriver implements GlobalConst {

    private int TRUE = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;
    private String dbfilename = "data.in";
    private String datafile;
    private int numAttribs;
    AttrType[] attrType;
    private Args args;

    /**
     * BMDriver Constructor, inherited from TestDriver
     */
    public Ph2Driver(Args _args) {
        super("phase2test");
        args = _args;
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

        /* load data into Minibase DB */

        boolean _pass = loadDataIntoDB();
        if (_pass) {
            //Run the tests. Return type different from C++
            _pass = runAllTests();
        }

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);

        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.print("\n" + "..." + args.skylineMethod + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    protected boolean loadDataIntoDB() {
        boolean status = OK;

        try (BufferedReader br = new BufferedReader(new FileReader(args.datafile))) {
            String line;
            line = br.readLine();
            numAttribs = Integer.parseInt(line.trim());
            //System.out.println("Number of data attributes: " + numAttribs);
            attrType = new AttrType[numAttribs];
            //short[] attrSize = new short[numAttribs];
            for (int i = 0; i < numAttribs; ++i) {
                attrType[i] = new AttrType(AttrType.attrReal);
            }


            Tuple t = new Tuple();
            try {
                t.setHdr((short) numAttribs, attrType, null);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            int size = t.size();

            RID rid;
            Heapfile f = null;
            try {
                f = new Heapfile(dbfilename);
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile constructor ***");
                status = FAIL;
                e.printStackTrace();
            }

            t = new Tuple(size);
            try {
                t.setHdr((short) numAttribs, attrType, null);
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
                        t.setFloFld(i+1, Float.parseFloat(attrStr[i]));
                    }
                    catch (Exception e) {
                        System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                /* insert tuple into heapfile */
                try {
                    rid = f.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    System.err.println("*** error in Heapfile.insertRecord() ***");
                    status = FAIL;
                    e.printStackTrace();
                }

                ++num_tuples;
            }
            System.err.println("Number of tuples added to DB: " + num_tuples);
        }
        catch (IOException e) {
            status = FAIL;
            e.printStackTrace();
        }

        /* index stage */
        return status;
    }
    protected boolean runAllTests() {

        boolean _passAll = OK;

        switch (args.skylineMethod) {
            case "nested":
                _passAll = nestedSkylineTest();
                break;
            case "blocknested":
                _passAll = blockNestedSkylineTest();
                break;
            case "sortfirst":
                _passAll = sortFirstSkylineTest();
                break;
            case "btree":
                _passAll = bTreeSkylineTest();
                break;
            case "btreesort":
                //_passAll = bTreeSortedSkylineTest();
                break;
            case "all":
                _passAll = nestedSkylineTest() && blockNestedSkylineTest() && sortFirstSkylineTest()
                            && bTreeSkylineTest() /* && bTreeSortedSkylineTest() */;
                break;

        }

        //Running test1() to test6()
        /*
        if (!test1()) {
            _passAll = FAIL;
        }

        if (!test2()) {
            _passAll = FAIL;
        }

        if (!test3()) {
            _passAll = FAIL;
        }
        /*
        if (!test4()) {
            _passAll = FAIL;
        }
        if (!test5()) {
            _passAll = FAIL;
        }
        if (!test6()) {
            _passAll = FAIL;
        }
        */

        return _passAll;
    }

    /* Tests NestedLoopSky() skyline computation */
    public boolean nestedSkylineTest() {
        boolean status = OK;

        /*
        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;
         */

        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        // create a scan
        FileScan scan = null;
        try {
            scan  = new FileScan(dbfilename, attrType, null,
                    (short)numAttribs, (short)numAttribs, proj_list, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("Failed to create file scan: " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        NestedLoopsSky nls = null;
        try {
            nls = new NestedLoopsSky(attrType, attrType.length, scan,
                    dbfilename, args.pref_attribs, args.n_pages);

        } catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple t;
        int num_skylines = 0;
        try {
            while ((t = nls.get_next()) != null) {
                num_skylines++;
                System.out.println("skylines retrieved: " + num_skylines);
                int num_fields = t.noOfFlds();
                for (int i=0; i < num_fields; ++i) {
                    System.out.print("" + t.getFloFld(i+1) + " ");
                }
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("Number of skylines: " + num_skylines);

        // clean up
        try {
            nls.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return status;
    }

    /* BlockNestedLoopsSky test */
    public boolean blockNestedSkylineTest() {
        System.out.println("\nFinding skylines using BlockNestedLoopsSky operator...\n");
        boolean status = OK;
        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;

        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        // create a scan
        FileScan scan = null;
        try {
            scan  = new FileScan(dbfilename, attrType, null,
                    (short)numAttribs, (short)numAttribs, proj_list, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("Failed to create file scan: " + e);
        }

        BlockNestedLoopsSky blockNestedLoopsSky = null;
        try {
            blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, attrType.length,
                    new short[0], scan, dbfilename, args.pref_attribs, args.pref_attribs.length, args.n_pages);
        } catch (Exception e) {
            System.err.println("*** Error preparing for Block nested loop skyline computation");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple t;
        int num_skylines = 0;
        //System.out.println("Preferred Attributes: " + Arrays.toString(args.pref_attribs));
        System.out.println("Skylines:");
        try {
            while ((t = blockNestedLoopsSky.get_next()) != null) {
                num_skylines++;
                //System.out.println("skylines retrieved: " + num_skylines);
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);

        // clean up
        try {
            blockNestedLoopsSky.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return status;
    }

    public boolean sortFirstSkylineTest() {
        System.out.println("\nFinding skylines using SortFirstSky operator...\n");
        boolean status = OK;
        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;

        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        // create a scan
        FileScan scan = null;
        try {
            scan  = new FileScan(dbfilename, attrType, null,
                    (short)numAttribs, (short)numAttribs, proj_list, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("Failed to create file scan: " + e);
        }

        SortFirstSky sfs = null;
        try {
            sfs = new SortFirstSky(attrType, attrType.length, new short[0],
                    scan, dbfilename, args.pref_attribs, args.pref_attribs.length, args.n_pages);
        } catch (Exception e) {
            System.err.println("*** Error preparing for SortFirstSky skyline computation");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple t;
        int num_skylines = 0;
        System.out.println("Preferred Attributes: " + Arrays.toString(pref_list));
        System.out.println("Skylines:");
        try {
            while ((t = sfs.get_next()) != null) {
                num_skylines++;
                //System.out.println("skylines retrieved: " + num_skylines);
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);

        // clean up
        try {
            sfs.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return status;
    }

    /* BTreeSky operator test */
    public boolean bTreeSkylineTest() {
        System.out.println("\ntest3(): BTreeSky ...\n");
        boolean status = OK;
        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;

        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        // create a scan
        FileScan scan = null;
        try {
            scan  = new FileScan(dbfilename, attrType, null,
                    (short)numAttribs, (short)numAttribs, proj_list, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("Failed to create file scan: " + e);
        }

        SortFirstSky sfs = null;
        try {
            sfs = new SortFirstSky(attrType, attrType.length, new short[0],
                    scan, dbfilename, args.pref_attribs, args.pref_attribs.length, args.n_pages);
        } catch (Exception e) {
            System.err.println("*** Error preparing for SortFirstSky skyline computation");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple t;
        int num_skylines = 0;
        System.out.println("Preferred Attributes: " + Arrays.toString(pref_list));
        System.out.println("Skylines:");
        try {
            while ((t = sfs.get_next()) != null) {
                num_skylines++;
                //System.out.println("skylines retrieved: " + num_skylines);
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);

        // clean up
        try {
            sfs.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return status;
    }

    /* BTreeSortedSky operator test */
    /*
    public boolean bTreeSortedSkylineTest() {
        System.out.println("\ntest3(): BTreeSortedSky ...\n");
        boolean status = OK;
        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;

        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        // create a scan
        FileScan scan = null;
        try {
            scan  = new FileScan(dbfilename, attrType, null,
                    (short)numAttribs, (short)numAttribs, proj_list, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("Failed to create file scan: " + e);
        }

        BTreeSortedSky btss = null;
        try {
            btss = new BTreeSortedSky(attrType, attrType.length, new short[0],
                    scan, dbfilename, args.pref_attribs, args.pref_attribs.length, args.n_pages);
        } catch (Exception e) {
            System.err.println("*** Error preparing for SortFirstSky skyline computation");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        Tuple t;
        int num_skylines = 0;
        System.out.println("Preferred Attributes: " + Arrays.toString(pref_list));
        System.out.println("Skylines:");
        try {
            while ((t = btss.get_next()) != null) {
                num_skylines++;
                //System.out.println("skylines retrieved: " + num_skylines);
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);

        // clean up
        try {
            btss.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return status;
    }
    */

    private void printTuple(Tuple t) throws Exception {
        int num_fields = t.noOfFlds();
        for (int i=0; i < num_fields; ++i) {
            System.out.printf("%f ", t.getFloFld(i+1));
        }
        System.out.println("");
    }

    /**
     * overrides the testName function in TestDriver
     *
     * @return the name of the test
     */
    protected String testName() {
        return "Phase2Test";
    }
}


public class Phase2Test {

    private static final int ARGS_COUNT = 4;

    private  static  void usage() {
        System.out.println("\njava tests.Phase2Test " +
                "-datafile=<datafile> " +
                "-skyline=<nested | blocknested | sortfirst | btree | btreesort | all> " +
                "-attr=<comma separated numbers> " +
                "-npages=<number-of-pages>\n");
    }
    private static boolean skylineMethodValid(String skyline) {
        String[] validSkylines = {"nested", "blocknested", "sortfirst", "btree", "btreesort", "all"};
        for (String method : validSkylines) {
            if (method.equals(skyline)) {
                return true;
            }
        }
        return false;
    }

    private static Args parseArgs(String[] args) {
        Args pa = new Args();
        int num_args = 0;
        for(String arg : args) {
            if (arg.startsWith("-datafile=")) {
                pa.datafile = arg.substring(new String("-datafile=").length());
                num_args++;
            } else if (arg.startsWith("-skyline=")) {
                pa.skylineMethod = arg.substring(new String("-skyline=").length());
                if (skylineMethodValid(pa.skylineMethod) == false) {
                    System.err.println("Invalid skyline operator: " + pa.skylineMethod);
                    return null;
                }
                num_args++;
            } else if (arg.startsWith("-attr=")) {
                String[] attribs = arg.substring(new String("-attr=").length()).split(",", 0);
                pa.pref_attribs = new int[attribs.length];
                for (int i=0; i<attribs.length; ++i) {
                    pa.pref_attribs[i] = Integer.parseInt(attribs[i]);
                }
                num_args++;
            } else if (arg.startsWith("-npages=")) {
                pa.n_pages = Integer.parseInt(arg.substring(new String("-npages=").length()));
                num_args++;
            }

        }

        if (num_args != ARGS_COUNT) {
            System.err.println("invalid arguments: " + Arrays.toString(args));
            return null;
        }
        return pa;
    }

    public static void main(String[] args) {

        if (args.length < ARGS_COUNT) {
            System.out.println("Too few arguments given. See usage below:");
            usage();
            Runtime.getRuntime().exit(1);
        }
        Args pa = parseArgs(args);
        if (pa == null) {
            usage();
            Runtime.getRuntime().exit(1);
        }
        System.err.println("datafile: " + pa.datafile);
        System.err.println("skyline: " + pa.skylineMethod);
        System.err.println("preferred attributes: " + Arrays.toString(pa.pref_attribs));
        System.err.println("n_pages: " + pa.n_pages);

        Ph2Driver ph2Driver = new Ph2Driver(pa);
        boolean dbstatus;

        dbstatus = ph2Driver.runTests();

        if (!dbstatus) {
            System.out.println("Error encountered during Dominates tests:\n");
            Runtime.getRuntime().exit(1);
        }
        System.out.println("Number of pages read: " + PageCounter.getReadCounter());
        System.out.println("Number of pages written: " + PageCounter.getWriteCounter());

        Runtime.getRuntime().exit(0);
    }
}