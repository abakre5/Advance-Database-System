package tests;

import btree.*;
import global.*;
import heap.*;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.*;
import java.io.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class Player {
    public Float pid;
    public Float goals;
    public Float assists;

    public Player( Float _pid, Float _goals, Float _assists ) {
        pid = _pid;
        goals = _goals;
        assists = _assists;
    }
}

class BTreeSortedSkyDriver extends TestDriver implements GlobalConst {
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
    public BTreeSortedSkyDriver() {
        super("BTreeSortedSkyTest");
    }

    /**
     * calls the runTests function in TestDriver
     */
    public boolean runTests() {

        System.out.print("\n" + "Running " + testName() + " tests...." + "\n");

        try {
            SystemDefs sysdef = new SystemDefs(dbpath, NUMBUF + 2500, NUMBUF, "Clock");
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
    
        AttrType[] Ptypes = new AttrType[2];
        Ptypes[0] = new AttrType (AttrType.attrReal);
        Ptypes[1] = new AttrType (AttrType.attrReal);
        // Ptypes[2] = new AttrType (AttrType.attrReal);
        // Ptypes[3] = new AttrType (AttrType.attrReal);
        // Ptypes[4] = new AttrType (AttrType.attrReal);
    
        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN2;
        attrSize[1] = REC_LEN1;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 2,Ptypes, attrSize);
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
            f = new Heapfile("btreesorted.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }
    
        t = new Tuple(size);
        try {
            t.setHdr((short) 2, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }
    
        try (BufferedReader br = new BufferedReader(new FileReader("data_large_skyline.txt"))) {
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
    
        FldSpec[] Pprojection = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        Pprojection[0] = new FldSpec(rel, 1);
        Pprojection[1] = new FldSpec(rel, 2);
        // Pprojection[2] = new FldSpec(rel, 3);
        // Pprojection[3] = new FldSpec(rel, 4);
        // Pprojection[4] = new FldSpec(rel, 5);
        // Scan the players table
        FileScan am = null;
        try {
            am  = new FileScan("btreesorted.in", Ptypes, attrSize,
                    (short)2, (short)2,
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
      //  pref_list[1] = 3;

        // create the index file
        BTreeFile btf1 = null;
        try {
            btf1 = new BTreeFile("BTreeIndexNew", AttrType.attrReal, 4, 1/*delete*/);
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
        Float flo = (Float)0.1f;
        while (temp != null) {
            t.tupleCopy(temp);

            try {
                //key = t.getFloFld( pref_list[0]) + t.getFloFld( pref_list[1]) ;
                key = t.getFloFld( pref_list[0]);
           
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                btf1.insert(new FloatKey(key), rid);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                temp = scan.getNext(rid);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        try {
           // BT.printAllLeafPages(btf1.getHeaderPage());
        }catch (Exception e){
            e.printStackTrace();
        }

        IndexFile[] indexFiles = new IndexFile[1];
        indexFiles[0] = btf1;
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        
       //Get skyline elements
        BTreeSortedSky bTreeSortedSky = null;
        try {
            bTreeSortedSky = new BTreeSortedSky(Ptypes, Ptypes.length, null, 1000, am, "btreesorted.in", pref_list, null, indexFiles, 5);
        } catch (Exception e) {
            System.err.println ("*** Error preparing for btree sorted sky");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        t = new Tuple();
        try {
            System.out.println("setsky: calling");
            t = bTreeSortedSky.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.out.println("-----------Skyline for given dataset--------------");
        while( t != null ) {
           
            try {
                //System.out.println(t.getFloFld(1) + " -- " + t.getFloFld(2) + " " + t.getFloFld(3) + " " +  t.getFloFld(4) + " "+ t.getFloFld(5));
                System.out.println(t.getFloFld(1) + " -- " + t.getFloFld(2));
                t = bTreeSortedSky.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        // if(t==null) {
        //     System.out.println("setsky: all null");
        // }
        System.out.println("-------------End Skyline------------------");

        //clean up
        try {
            bTreeSortedSky.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        long actualMemUsed=afterUsedMem-beforeUsedMem;
        System.out.println("Memory usage : "+actualMemUsed/(1024*1024));
        System.out.println("------------------- TEST 1 completed ---------------------\n");
        return status;
    }
    
}

public class BTreeSortedSkyTest {
    public static void main(String[] args) {

        BTreeSortedSkyDriver nlsDriver = new BTreeSortedSkyDriver();
        boolean dbstatus;

        dbstatus = nlsDriver.runTests();

        if (!dbstatus) {
            System.out.println("Error encountered during Dominates tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}