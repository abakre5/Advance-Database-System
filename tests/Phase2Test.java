package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.time.*;

import btree.BTreeFile;
import btree.FloatKey;
import btree.IndexFile;
import btree.KeyDataEntry;
import chainexception.ChainException;
import diskmgr.Page;
import diskmgr.PageCounter;
import global.*;
import heap.*;
import index.IndexUtils;
import iterator.*;
import hash.HashFile;
import hash.HashIndexFileScan;
import hash.HashUnclusteredFileScan;
import hash.UnclusteredHashData;

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
    private Heapfile dbHeapFile;
    AttrType[] attrType;
    private Args args;
    private int initialReads = 0;
    private int initialWrites = 0;
    int num_records;

    /**
     * BMDriver Constructor, inherited from TestDriver
     */
    public Ph2Driver(Args _args) {
        super("phase2test");
        args = _args;
        initialReads = 0;
        initialWrites = 0;
    }

    /**
     * calls the runTests function in TestDriver
     */
    public boolean runTests() {


        System.out.print("\n" + "Running " + testName() + " tests...." + "\n");

        try {
            SystemDefs sysdef = new SystemDefs(dbpath, MINIBASE_DB_SIZE, NUMBUF, "Clock");
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
            PageCounter.init();
            line = br.readLine();
            numAttribs = Integer.parseInt(line.trim());
            System.out.println("Number of data attributes: " + numAttribs);
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
                dbHeapFile = new Heapfile(dbfilename);
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
                        System.err.println("*** Heapfile error in Tuple.setFloFld() ***");
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                /* insert tuple into heapfile */
                try {
                    rid = dbHeapFile.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    System.err.println("*** error in Heapfile.insertRecord() ***");
                    status = FAIL;
                    e.printStackTrace();
                }

                ++num_tuples;
            }
            initialReads = PageCounter.getReadCounter();
            initialWrites = PageCounter.getWriteCounter();
            System.out.println(initialReads + " " + initialWrites);
            System.out.println("Number of tuples added to DB: " + num_tuples + "\n");
            num_records = num_tuples;
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

        Instant start = Instant.now();

        switch (args.skylineMethod) {
            // case "nested":
            //     _passAll = nestedSkylineTest();
            //     break;
            // case "blocknested":
            //     _passAll = blockNestedSkylineTest();
            //     break;
            // case "sortfirst":
            //     _passAll = sortFirstSkylineTest();
            //     break;
            // case "btree":
            //     _passAll = bTreeSkylineTest();
            //     break;
            // case "btreesort":
            //     _passAll = bTreeSortedSkylineTest();
            //    break;
            case "hash_index":
                _passAll = HashFileTestFunctTest();
            // case "all":
            //     _passAll = nestedSkylineTest() && blockNestedSkylineTest() && sortFirstSkylineTest()
            //             && bTreeSkylineTest()  && bTreeSortedSkylineTest();
                break;

        }

        if (_passAll) {
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.printf("Time Elapsed: %.3fs\n", (float)timeElapsed/1000, timeElapsed);
        }

        return _passAll;
    }

    /* Tests NestedLoopSky() skyline computation */
    public boolean nestedSkylineTest() {
        System.out.println("---------------------------------------------------------------------------------------------------------\n");
        System.out.println("Finding skylines using Nested Loops Skyline operator...\n");
        boolean status = OK;

        System.out.println("\nFinding skylines using NestedLoopSky iterator...\n");

        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        // create a scan
        FileScan scan = null;
        PageCounter.init();
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
            nls = new NestedLoopsSky(attrType, attrType.length, new short[0], scan,
                    dbfilename, args.pref_attribs, args.pref_attribs.length, args.n_pages);

        } catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("\nSkyline computation completed using Nested Loops Skyline operator ...\nFollowing is the result");
        Tuple t;
        int num_skylines = 0;
        try {
            while ((t = nls.get_next()) != null) {
                num_skylines++;
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);

        printReadAndWrites();
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
        System.out.println("---------------------------------------------------------------------------------------------------------\n");
        System.out.println("Finding skylines using BlockNestedLoopsSky operator...\n");
        boolean status = OK;

        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }
        // create a scan
        FileScan scan = null;
        PageCounter.init();
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
        System.out.println("\nSkyline computation completed using Block Nested Loops Skyline operator ...\nFollowing is the result");
        int num_skylines = 0;
        //System.out.println("Preferred Attributes: " + Arrays.toString(args.pref_attribs));
        System.out.println("Skylines:");
        try {
            while ((t = blockNestedLoopsSky.get_next()) != null) {
                num_skylines++;
                //printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);

        printReadAndWrites();
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
        System.out.println("---------------------------------------------------------------------------------------------------------\n");
        System.out.println("Finding skylines using SortFirstSky operator...\n");
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
        PageCounter.init();
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


        System.out.println("\nSkyline computation completed using Sort First Skyline operator ...\nFollowing is the result");
        Tuple t;
        int num_skylines = 0;
        //System.out.println("Preferred Attributes: " + Arrays.toString(pref_list));
        System.out.println("Skylines:");
        try {
            while ((t = sfs.get_next()) != null) {
                num_skylines++;
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);

        printReadAndWrites();
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
        System.out.println("---------------------------------------------------------------------------------------------------------\n");
        System.out.println("Finding skylines using BTreeSky operator...\n");
        boolean status = OK;


        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        // create a scan
        FileScan scan = null;
        PageCounter.init();
        try {
            scan  = new FileScan(dbfilename, attrType, null,
                    (short)numAttribs, (short)numAttribs, proj_list, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("Failed to create file scan: " + e);
        }

        // create an scan on the heapfile
        Scan heapfile_scan = null;

        try {
            heapfile_scan = new Scan(dbHeapFile);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile[] btf = new BTreeFile[args.pref_attribs.length];
        try {
            for(int  i = 0 ; i < args.pref_attribs.length; i++){
                btf[i] = new BTreeFile("BTreeIndex" + i, AttrType.attrReal, 4, 1/*delete*/);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("BTreeIndex created on each pref attributes...\n");

        RID rid = new RID();
        Float key = null;

        Tuple temp;
        Tuple t;

        try {
            while ((temp = heapfile_scan.getNext(rid)) != null) {

                temp.setHdr((short)numAttribs, attrType, null);

                for (int  i = 0 ; i < args.pref_attribs.length; i++) {
                    try {
                        key = temp.getFloFld(args.pref_attribs[i]);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }

                    try {
                        btf[i].insert(new FloatKey(key), rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        BTreeSky bts = null;
        try {
            bts = new BTreeSky(attrType, attrType.length, null, scan,
                    dbfilename, args.pref_attribs, null, btf, args.n_pages);
        } catch (Exception e) {
            System.err.println("*** Error preparing for BTreeSky skyline computation");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("\nSkyline computation completed using BTree Skyline operator ...\nFollowing is the result");
        int num_skylines = 0;
        System.out.println("Skylines:");
        try {
            while ((t = bts.get_next()) != null) {
                num_skylines++;
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);
        printReadAndWrites();

        // clean up
        try {
            bts.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return status;
    }


    public boolean  HashFileTestFunctTest(){
        System.out.println("-----------------------------------------------Reached----------------------------------------------------------\n");
        boolean status = OK;


        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i = 0; i < numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        /* initialize PageCounter to track Page reads/writes */
        PageCounter.init();
        // create a scan
        FileScan scan = null;
        try {
            scan = new FileScan(dbfilename, attrType, null,
                    (short) numAttribs, (short) numAttribs, proj_list, null);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("Failed to create file scan: " + e);
        }

        // create an scan on the heapfile
        Scan heapfile_scan = null;

        try {
            heapfile_scan = new Scan(dbHeapFile);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        HashFile hf = null;

        try {
            hf = new HashFile("nc_2_7000_single.txt", "HashIndex", 1, AttrType.attrReal, scan, num_records, dbHeapFile);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Starting insert test");

        try {
            Heapfile hf1 = new Heapfile("pc_dec_2_500_multi11.txt");
            RID rid = null;
            proj_list = new FldSpec[numAttribs];
            for (int i = 0; i < numAttribs; ++i) {
                proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

            /* initialize PageCounter to track Page reads/writes */
            PageCounter.init();
            // create a scan
            scan = null;
            try {
                scan = new FileScan(dbfilename, attrType, null,
                        (short) numAttribs, (short) numAttribs, proj_list, null);
            } catch (Exception e) {
                status = FAIL;
                System.err.println("Failed to create file scan: " + e);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        TupleRIDPair record = null;
        Tuple data = null;
        RID rid = null;
        int count = 0;
        do {


            try {


                record = scan.get_next1();
                if (record != null) {
                    data = record.getTuple();
                    rid = record.getRID();
                } else {
                    break;
                }
                AttrType[] attrTypes = new AttrType[5];
                //short[] attrSize = new short[numAttribs];
                for (int i = 0; i < 2; ++i) {
                    attrTypes[i] = new AttrType(AttrType.attrReal);
                }
                try {
                    data.setHdr((short) 2, attrTypes, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                rid = dbHeapFile.insertRecord(data.getTupleByteArray());

                hf.insert(new hash.FloatKey(data.getFloFld(1)), rid);
                count++;
                //System.out.println("Inserted");

            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (count <= 420);

        try {
            //hf.printindex();
            System.out.println("=======================================================");
            //hf.printHeaderFile();
        } catch (Exception e) {
            e.printStackTrace();
        }


    //Scanner
    int elem_count = 0;
    HashIndexFileScan hashScan = new HashUnclusteredFileScan();
    hash.KeyClass key = null;
    try {
    hashScan = (HashUnclusteredFileScan)IndexUtils.HashUnclusteredScan(hf);
    hash.KeyDataEntry entry = hashScan.get_next();
        while(entry!=null) {
            if(entry!=null) {
            RID fetchRID = entry.data;
            System.out.println("\n\n Output :"+fetchRID.pageNo.pid + " "+fetchRID.slotNo);
            Tuple t = dbHeapFile.getRecord(fetchRID);
            Tuple current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
            attrType = new AttrType[2];
            //short[] attrSize = new short[numAttribs];
            for (int i = 0; i < 2; ++i) {
                attrType[i] = new AttrType(AttrType.attrReal);
            }
            current_tuple.setHdr((short)2, attrType, null);

            System.out.println(current_tuple.getFloFld(1) + " " + current_tuple.getFloFld(2));
            key = new hash.FloatKey(current_tuple.getFloFld(1));
            elem_count++;
            entry = hashScan.get_next();
        
        System.out.println("Total elements scanned "+ elem_count);
        System.out.println("\n\n");
            
        if (elem_count  == 100 || elem_count == 190 || elem_count == 755){
            System.out.println("Testing delete");
            boolean del = hf.delete(key);
            if (del){
                System.out.println("Delete: "+ key+" deleted successfully");
            } else {
                System.out.println("Delete: "+ key+" Could not be deleted successfully");
            }
           
        } else {
            System.out.println("Testing search");
            Tuple tt = hf.search(key);

            if(tt!=null) {
                System.out.println("Found.......................................\n");
                System.out.println(tt.getFloFld(1)+" "+tt.getFloFld(2));
                //hf.printHeaderFile();
            } else{
                System.out.println("******************Not Found : Breaking ********************\n");
                //hf.printHeaderFile();
                System.out.println("..................Not Found.......................................\n");
                //hf.printindex();
                break;   
            } 
        } 
          
    } 
}
    // hf.printindex();
    hf.printHeaderFile();
    hf.printMetadataFile();
} catch(Exception e) {
        e.printStackTrace();
    }
    return true;
    }

    /* BTreeSortedSky operator test */
    public boolean bTreeSortedSkylineTest() {
        System.out.println("---------------------------------------------------------------------------------------------------------\n");
        System.out.println("Finding skylines using BTreeSortedSky operator...\n");
        boolean status = OK;


        FldSpec[] proj_list = new FldSpec[numAttribs];
        for (int i=0; i<numAttribs; ++i) {
            proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }

        /* initialize PageCounter to track Page reads/writes */
        PageCounter.init();
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

        // create an scan on the heapfile
        Scan heapfile_scan = null;

        try {
            heapfile_scan = new Scan(dbHeapFile);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        try {
            btf = new BTreeFile("BTreeIndexNew", AttrType.attrReal, 4, 1/*delete*/);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("BTreeIndex created on each pref attributes...\n");

        RID rid = new RID();
        Float key = null;

        Tuple temp;
        Tuple t;

        try {
            while ((temp = heapfile_scan.getNext(rid)) != null) {

                temp.setHdr((short)numAttribs, attrType, null);
                key = 0f;
                for (int  i = 0 ; i < args.pref_attribs.length; i++) {
                    try {
                        key += temp.getFloFld(args.pref_attribs[i]);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                }
                btf.insert(new FloatKey(key), rid);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        IndexFile[] indexFiles = new IndexFile[1];
        indexFiles[0] = btf;

        /* create BTreeSortedSky iterator */
        BTreeSortedSky btss = null;
        try {
            btss = new BTreeSortedSky(attrType, attrType.length, null, 1000, scan,
                    dbfilename, args.pref_attribs, null, indexFiles, args.n_pages);
        } catch (Exception e) {
            System.err.println("*** Error preparing for BTreeSortedSky skyline computation");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        int num_skylines = 0;
        System.out.println("\nSkyline computation completed using BTree sorted Skyline operator ...\nFollowing is the result");
        System.out.println("Skylines:");
        try {
            while ((t = btss.get_next()) != null) {
                num_skylines++;
                printTuple(t);
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\nNumber of skylines: " + num_skylines);
        printReadAndWrites();

        // clean up
        try {
            btss.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return status;
    }

    private void printTuple(Tuple t) throws Exception {
        int num_fields = t.noOfFlds();
        for (int i=0; i < num_fields; ++i) {
            System.out.printf("%f ", t.getFloFld(i+1));
        }
        System.out.println("");
    }

    private void printReadAndWrites() {
        System.out.println("Number of pages read: " + PageCounter.getReadCounter());
        System.out.println("Number of pages written: " + PageCounter.getWriteCounter());
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
                "-skyline=<nested | blocknested | sortfirst | btree | btreesort | hash_index | all> " +
                "-attr=<comma separated numbers> " +
                "-npages=<number-of-pages>\n");
    }
    private static boolean skylineMethodValid(String skyline) {
        String[] validSkylines = {"nested", "blocknested", "sortfirst", "btree", "btreesort","hash_index","all"};
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
        if (pa.n_pages < 1) {
            System.err.println("n_pages attribute should be greater than 0.");
            return;
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

        Runtime.getRuntime().exit(0);
    }
}