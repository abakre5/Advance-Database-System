package tests;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.SortFirstSky;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class testes the SortFirstSkyline implementation for the given data.
 * @author manthan agrawal magraw12@asu.edu
 * @author abhishek bakre abakre1@asu.edu
 */
class SortFirstSkyDriver extends TestDriver implements GlobalConst {
    private final boolean OK = true;
    private final boolean FAIL = false;

    /**
     * SortFirstSkyDriver Constructor, inherited from TestDriver
     */
    public SortFirstSkyDriver() {
        super("SortFirstSkylineTest");
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

        //Running test1()
        if (!test1()) {
            _passAll = FAIL;
        }

        return _passAll;
    }

    public boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");
        boolean status = OK;

        // Add actual players list
        List<Player> playersList = new ArrayList<>();
        int numPlayers = 8;

        playersList.add(new Player(108, 120, 310.0f));
        playersList.add(new Player(107, 500, 50.0f));
        playersList.add(new Player(106, 320, 150.0f));
        playersList.add(new Player(105, 410, 70.0f));
        playersList.add(new Player(104, 200, 130.0f));
        playersList.add(new Player(103, 210, 240.0f));
        playersList.add(new Player(102, 660, 90.0f));
        playersList.add(new Player(101, 340, 190.0f));

        AttrType[] Ptypes = new AttrType[3];
        Ptypes[0] = new AttrType(AttrType.attrInteger);
        Ptypes[1] = new AttrType(AttrType.attrInteger);
        Ptypes[2] = new AttrType(AttrType.attrReal);

        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, Ptypes, null);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("playersSortFirst.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Ptypes, null);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < numPlayers; i++) {
            try {
                t.setIntFld(1, playersList.get(i).pid);
                t.setIntFld(2, playersList.get(i).goals);
                t.setFloFld(3, playersList.get(i).assists);
            } catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

        FldSpec[] Pprojection = new FldSpec[3];
        Pprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Pprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        Pprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);

        // Scan the players table
        FileScan am = null;
        try {
            am = new FileScan("playersSortFirst.in", Ptypes, null,
                    (short) 3, (short) 3,
                    Pprojection, null);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("" + e);
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error setting up scan for players");
            Runtime.getRuntime().exit(1);
        }

        int[] pref_list = new int[3];
        pref_list[0] = 1;
        pref_list[1] = 2;
        pref_list[2] = 3;

        // Get skyline elements
        SortFirstSky sortFirstSkyline = null;
        try {
            sortFirstSkyline = new SortFirstSky(Ptypes, 3, new short[0], am, "playersSortFirst.in", pref_list, pref_list.length, 3);
        } catch (Exception e) {
            System.err.println("*** Error preparing for sort first skyline");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = new Tuple();
        int count = 0;
        try {
            t = sortFirstSkyline.get_next();
//            System.out.println(t.getIntFld(1));
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int NUM_SKYLINE_PLAYERS = 7;

        while (t != null) {
//            try {
//                System.out.println(t.getIntFld(1) + " : " + t.getIntFld(2) + " : " + t.getIntFld(3));
//            } catch (IOException | FieldNumberOutOfBoundException e) {
//                e.printStackTrace();
//            }
            if (count >= NUM_SKYLINE_PLAYERS) {
                System.err.println("Test1 -- OOPS! too many records");
                System.out.println(count);
                status = FAIL;
//        flag = false;
                break;
            }
            count++;

            try {
                t = sortFirstSkyline.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        System.out.println(count);
        if (count < NUM_SKYLINE_PLAYERS) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        } else if (status) {
            System.err.println("Test1 -- Sort First Skyline OK");
        }

        // clean up
        try {
            sortFirstSkyline.close();
        } catch (Exception e) {
            status = FAIL;
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
        return "SortFirstSkyTest";
    }
}

public class SortFirstSkyTest {
    public static void main(String[] args) {

        SortFirstSkyDriver sortFirstSkyDriver = new SortFirstSkyDriver();
        boolean dbstatus;

        dbstatus = sortFirstSkyDriver.runTests();

        if (!dbstatus) {
            System.out.println("Error encountered during Sort first Skyline tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}
