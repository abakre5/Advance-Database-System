package tests;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.NestedLoopsSky;
import iterator.RelSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class Player {
    public int pid;
    public int goals;
    public float assists;

    public Player( int pid, int goals, float assists ) {
        this.pid = pid;
        this.goals = goals;
        this.assists = assists;
    }
}

class NestedLoopsSkyDriver extends TestDriver implements GlobalConst {
    private int TRUE = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private ArrayList<Player> playersList;

    /**
     * NestedLoopsSkyDriver Constructor, inherited from TestDriver
     */
    public NestedLoopsSkyDriver() {
        super("NestedLoopsSkyTest");
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

        return _passAll;
    }

    public boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");
        boolean status = OK;

        // Add actual players list
        playersList = new ArrayList<>();
        int numPlayers = 8;

        playersList.add( new Player( 101, 340, 190.40f ) );
        playersList.add( new Player( 102, 460, 90.30f ) );
        playersList.add( new Player( 103, 210, 240.50f ) );
        playersList.add( new Player( 104, 200, 130.70f ) );
        playersList.add( new Player( 105, 410, 70.20f ) );
        playersList.add( new Player( 106, 320, 150.04f ) );
        playersList.add( new Player( 107, 500, 50.03f ) );
        playersList.add( new Player( 108, 120, 310.5f ) );

        AttrType[] Ptypes = new AttrType[3];
        Ptypes[0] = new AttrType (AttrType.attrInteger);
        Ptypes[1] = new AttrType (AttrType.attrInteger);
        Ptypes[2] = new AttrType (AttrType.attrReal);

        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3,Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("players.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<numPlayers; i++) {
            try {
                t.setIntFld(1, ((Player)playersList.get(i)).pid);
                t.setIntFld(2, ((Player)playersList.get(i)).goals);
                t.setFloFld(3, ((Player)playersList.get(i)).assists);
            }
            catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

        FldSpec[] Pprojection = new FldSpec[3];
        Pprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Pprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        Pprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);

        // Scan the players table
        FileScan am = null;
        try {
            am  = new FileScan("players.in", Ptypes, null,
                    (short)3, (short)3,
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

        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;

        // Get skyline elements
        NestedLoopsSky nls = null;
        try {
//            nls = new NestedLoopsSky(Ptypes, Ptypes.length, am, "players.in", pref_list, 10);
            nls = new NestedLoopsSky(Ptypes, Ptypes.length, new short[0], am, "players.in", pref_list, pref_list.length, 10);
        } catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = new Tuple();
        int count = 0;
        try {
            t = nls.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int NUM_SKYLINE_PLAYERS = 5;

        while( t != null ) {
            if (count >= NUM_SKYLINE_PLAYERS) {
                System.err.println("Test1 -- OOPS! too many records");
                status = FAIL;
                break;
            }
            count++;

            try {
                t = nls.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        if (count < NUM_SKYLINE_PLAYERS) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        }
        else if (status) {
            System.err.println("Test1 -- Nested Loop Skyline Test 1 OK");
        }

        // clean up
        try {
            nls.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        playersList = null;

        System.out.println("------------------- TEST 1 completed ---------------------\n");
        return status;
    }

    public boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");
        boolean status = OK;

        playersList = new ArrayList<>();
        int numPlayers = 8;

        playersList.add( new Player( 101, 210, 140.0f ) );
        playersList.add( new Player( 102, 360, 190.0f ) );
        playersList.add( new Player( 103, 410, 240.0f ) );
        playersList.add( new Player( 104, 200, 130.0f ) );
        playersList.add( new Player( 105, 430, 70.0f ) );
        playersList.add( new Player( 106, 320, 150.0f ) );
        playersList.add( new Player( 107, 500, 50.0f ) );
        playersList.add( new Player( 108, 120, 310.0f ) );

        AttrType[] Ptypes = new AttrType[3];
        Ptypes[0] = new AttrType (AttrType.attrInteger);
        Ptypes[1] = new AttrType (AttrType.attrInteger);
        Ptypes[2] = new AttrType (AttrType.attrReal);

        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3,Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("players2.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<numPlayers; i++) {
            try {
                t.setIntFld(1, ((Player)playersList.get(i)).pid);
                t.setIntFld(2, ((Player)playersList.get(i)).goals);
                t.setFloFld(3, ((Player)playersList.get(i)).assists);
            }
            catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

        FldSpec[] Pprojection = new FldSpec[3];
        Pprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Pprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        Pprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);

        // Scan the players table
        FileScan am = null;
        try {
            am  = new FileScan("players2.in", Ptypes, null,
                    (short)3, (short)3,
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

        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;

        // Get skyline elements
        NestedLoopsSky nls = null;
        try {
            nls = new NestedLoopsSky(Ptypes, Ptypes.length, new short[0], am, "players2.in", pref_list, pref_list.length, 10);
        } catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = new Tuple();
        int count = 0;
        try {
            t = nls.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int NUM_SKYLINE_PLAYERS = 4;

        while( t != null ) {
            if (count >= NUM_SKYLINE_PLAYERS) {
                System.err.println("Test1 -- OOPS! too many records");
                status = FAIL;
                break;
            }
            count++;

            try {
                t = nls.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        if (count < NUM_SKYLINE_PLAYERS) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        }
        else if (status) {
            System.err.println("Test1 -- Nested Loop Skyline Test 2 OK");
        }

        // clean up
        try {
            nls.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        playersList = null;

        System.out.println("------------------- TEST 2 completed ---------------------\n");
        return status;
    }

    /**
     * overrides the testName function in TestDriver
     *
     * @return the name of the test
     */
    protected String testName() {
        return "NestedLoopsSky";
    }
}

public class NestedLoopsSkyTest {
    public static void main(String[] args) {

        NestedLoopsSkyDriver nlsDriver = new NestedLoopsSkyDriver();
        boolean dbstatus;

        dbstatus = nlsDriver.runTests();

        if (!dbstatus) {
            System.out.println("Error encountered during Dominates tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}
