package tests;

import btree.*;
import global.*;
import heap.*;
import index.IndexScan;
import iterator.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

class Player {
    public int pid;
    public int goals;
    public int assists;

    public Player( int _pid, int _goals, int _assists ) {
        pid = _pid;
        goals = _goals;
        assists = _assists;
    }
}

class BTreeSkyDriver extends TestDriver implements GlobalConst {
    private int TRUE = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private List playersList;

    /**
     * BTreeSkyDriver Constructor, inherited from TestDriver
     */
    public BTreeSkyDriver() {
        super("BTreeSkyTest");
    }

    /**
     * calls the runTests function in TestDriver
     */
    public boolean runTests() {

        System.out.print("\n" + "Running " + testName() + " tests...." + "\n");

        try {
            SystemDefs sysdef = new SystemDefs(dbpath, NUMBUF + 1000, NUMBUF, "Clock");
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

// Add actual players list
        playersList = new ArrayList();
        int numPlayers = 12;

//        playersList.add( new Player( 101, 340, 190 ) );
//        playersList.add( new Player( 102, 460, 90 ) );
//        playersList.add( new Player( 103, 210, 240 ) );
//        playersList.add( new Player( 104, 200, 130 ) );
//        playersList.add( new Player( 105, 410, 70 ) );
//        playersList.add( new Player( 106, 320, 150 ) );
//        playersList.add( new Player( 107, 500, 50 ) );
//        playersList.add( new Player( 108, 120, 310 ) );
//        playersList.add( new Player( 109, 410, 70 ) );
//        playersList.add( new Player( 110, 20, 10 ) );
//        playersList.add( new Player( 111, 50, 20 ) );
//        playersList.add( new Player( 112, 110, 30 ) );


        playersList.add( new Player( 1, 25,  7) );
        playersList.add( new Player( 3, 27, 10) );
        playersList.add( new Player( 25, 30,  3) );
        playersList.add( new Player( 2, 35, 2 ) );
        playersList.add( new Player( 35, 40, 3 ) );
        playersList.add( new Player( 17, 70, 1) );
//
//        for (int  i= 0 ; i < 1000; i++){
//            playersList.add( new Player( i, i, 1001-i) );
//        }

        AttrType[] Ptypes = new AttrType[5];
        Ptypes[0] = new AttrType (AttrType.attrReal);
        Ptypes[1] = new AttrType (AttrType.attrReal);
        Ptypes[2] = new AttrType (AttrType.attrReal);
        Ptypes[3] = new AttrType (AttrType.attrReal);
        Ptypes[4] = new AttrType (AttrType.attrReal);

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
        try {
            f = new Heapfile("btreesky.in");
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
                        rid = f.insertRecord(t.returnTupleByteArray());
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
            am  = new FileScan("btreesky.in", Ptypes, attrSize,
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



        // create an scan on the heapfile
        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        int[] pref_list = new int[2];
        pref_list[0] = 2;
        pref_list[1] = 3;

        // create the index file
        BTreeFile[] btf = new BTreeFile[pref_list.length];
        try {
            for(int  i = 0 ; i < pref_list.length; i++){
                btf[i] = new BTreeFile("BTreeIndex" + i, AttrType.attrReal, REC_LEN1, 1/*delete*/);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("BTreeIndex created successfully.\n");

        rid = new RID();
        Float key = null;

        Tuple temp = null;

        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        while (temp != null)
        {
            t.tupleCopy(temp);
            for(int  i = 0 ; i < pref_list.length; i++) {
                try {
                    key = t.getFloFld(pref_list[i]);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                try {
                    btf[i].insert(new FloatKey(key), rid);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                temp = scan.getNext(rid);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        //IndexFile[] indexFiles = new IndexFile[pref_list.length];


        // Get skyline elements
        BTreeSky bTreeSky = null;
        try {
            bTreeSky = new BTreeSky(Ptypes, Ptypes.length, null, am, "btreesky.in", pref_list, null, (IndexFile[]) btf,  5);
        } catch (Exception e)
        {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = new Tuple();
        int count = 0;
        try {
            t = bTreeSky.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.out.println("-----------Skyline for given dataset--------------");
        status = true;
        while( t != null ) {

            try {
                System.out.println(t.getFloFld(1) + "   " +t.getFloFld(2) +  "   " + t.getFloFld(3)+ "  " + t.getFloFld(4) +" "+t.getFloFld(5));
                t = bTreeSky.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
        System.out.println("-----------End Skyline--------------");

        // clean up
        try {
            bTreeSky.close();
        }
        catch (Exception e) {
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
        return "BTreeSky";
    }
}

public class BTreeSkyTest {
    public static void main(String[] args) {

        BTreeSkyDriver nlsDriver = new BTreeSkyDriver();
        boolean dbstatus;

        dbstatus = nlsDriver.runTests();

        if (!dbstatus) {
            System.out.println("Error encountered during Dominates tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}
