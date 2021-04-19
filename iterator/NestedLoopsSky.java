package iterator;

import bufmgr.PageNotReadException;
import catalog.*;
import global.AttrType;
import global.ExtendedSystemDefs;
import global.GlobalConst;
import global.RID;
import heap.*;
import index.IndexException;
import tests.Phase3Utils;

import java.io.IOException;
import java.util.*;

/**
 * Created on 2/21/2021
 * @author Rahul Gore
 */
public class NestedLoopsSky extends Iterator {
    private AttrType[] in1;
    private int n_buf_pgs;
    private Iterator outer;
    private Iterator skylineItr;
    private int[] prefList;
    private List<Tuple> skyline;
    private boolean first_time;
    private String relationName;
    private short noOfColumns;
    private int noOfBufferPages;
    private List<RID> skylineRIDList;
    private Heapfile skyFile;
    private short[] strSizes;
    private int prefListLength;

    /**
     * Class constructor, take information about the tuples, and set up
     * the NestedLoopsSkyline algorithm
     *
     * @param in1               array containing attribute types of the relation
     * @param len_in1           number of columns in the relation
     * @param t1_str_sizes      array of sizes of string attributes
     * @param am1               an iterator for accessing the tuples
     * @param relationName      name of the heap file
     * @param pref_list         list of preference attributes
     * @param pref_list_length  number of preference attributes
     * @param n_pages           amount of memory (in pages) available for sorting
     * @throws Exception        exception from this class
     */
    public NestedLoopsSky(AttrType[] in1,
                          int len_in1,
                          short[] t1_str_sizes,
                          Iterator am1,
                          java.lang.String relationName,
                          int[] pref_list,
                          int pref_list_length,
                          int n_pages
    ) throws Exception {
        this.in1 = in1;
        this.noOfColumns = (short) len_in1;
        this.strSizes = t1_str_sizes;
        this.outer = am1;
        this.relationName = relationName;
        this.prefList = pref_list;
        this.prefListLength = pref_list_length;
        this.noOfBufferPages = n_pages;
        this.skylineRIDList = new ArrayList<>();
        this.skyFile = null;
        this.skylineItr = null;
        setSkyline();
    }

    /**
     * Computes the skyline of given relation based on given preference attributes
     * This method performs 2 scans - inner and outer - on the heap file; compares the tuples retrieved
     * from the scan; if a tuple is dominated by other, it is added to set of deleted tuples
     * Then, the tuples from the set are deleted from heapfile
     *
     * @throws Exception    exception from this class
     */
    private void setSkyline() throws Exception {
        if(!relationName.isEmpty()) {
            Heapfile origFile = new Heapfile(relationName);

            // IMP: allow the skyline operation only if sufficient pages are available
            int ridsPerPage = GlobalConst.MINIBASE_PAGESIZE/24;    /* 24 is the size of RID */
            int minPagesNeeded = (int) Math.ceil(origFile.getRecCnt() / ridsPerPage);
            if( noOfBufferPages < minPagesNeeded ) {
                System.out.println("*** ERROR! Insufficient number of pages:- minimum "+minPagesNeeded+" pages needed ***");
                return;
            }

            skyFile = new Heapfile("skynls.in");

            RID rid = new RID();
            FileScan ogScan = getFileScan(relationName);
            Tuple t = null;
            try {
                t = ogScan.get_next();
            } catch (Exception e) {
                System.err.println(" Could not set tuple");
                e.printStackTrace();
            }
            while(t != null) {
                try {
                    rid = skyFile.insertRecord(new Tuple(t).returnTupleByteArray());
                } catch (Exception e) {
                    e.printStackTrace();
                }

                t = ogScan.get_next();
            }
            ogScan.close();

            // Creating new scans for inner and outer loops
            FileScan outerScan = getFileScan("skynls.in");
            FileScan innerScan = getFileScan("skynls.in");

            // IMP: direct deletion causes InvalidSlotNumberException error
            // Solution: Keep deleted RIDs in array
            Set<RID> deletedRIDSet = new HashSet<>();

            TupleRIDPair outerTupleRid = outerScan.get_next1();
            do {
                RID outerRid = outerTupleRid.getRID();
                if( containsRid(deletedRIDSet, outerRid) ) {
                    outerTupleRid = outerScan.get_next1();
                    continue;
                }
                TupleRIDPair innerTupleRid = innerScan.get_next1();
                do {
                    Tuple outerTuple = outerTupleRid.getTuple();
                    Tuple innerTuple = innerTupleRid.getTuple();
                    RID innerRid = innerTupleRid.getRID();
                    if( containsRid(deletedRIDSet, innerRid) ) {
                        innerTupleRid = innerScan.get_next1();
                        continue;
                    }
                    if( TupleUtils.Dominates( outerTuple, in1, innerTuple, in1, (short)in1.length, new short[0], prefList, prefListLength ) ) {
                        try {
                            deletedRIDSet.add(innerRid);
                        } catch (Exception e) {
                            System.err.println("*** Error deleting record \n");
                            e.printStackTrace();
                            break;
                        }
                    }
                    innerTupleRid = innerScan.get_next1();
                } while(innerTupleRid != null);
                innerScan.close();
                innerScan = getFileScan("skynls.in");
                outerTupleRid = outerScan.get_next1();
            } while(outerTupleRid != null);
            outerScan.close();

            // None of the records were deleted yet.
            // Now delete the records; only skyline will remain
            for(RID deletedRID : deletedRIDSet) {
                skyFile.deleteRecord(deletedRID);
            }

            // Scan and store in skyline heap file
            skylineItr = getFileScan("skynls.in");
        } else {
            System.out.println("ERROR: Relation name not specified");
        }
    }

    /**
     * Checks if RID is present in given Set; the original HashSet 'contains' method
     * does not work properly
     *
     * @param ridSet    Set of RID elements
     * @param currRid   RID to be checked in the ridSet
     * @return          true if ridSet contains the currRid, false otherwise
     */
    private boolean containsRid(Set<RID> ridSet, RID currRid) {
        for( RID rid : ridSet ) {
            if ( currRid.equals(rid) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a FileScan on the given relation
     * @param relation              Name of the relation
     * @return                      FileScan on the given relation
     * @throws IOException          exception from this class
     * @throws FileScanException    exception from this class
     * @throws TupleUtilsException  exception from this class
     * @throws InvalidRelation      exception from this class
     */
    private FileScan getFileScan(String relation) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan;

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relation, in1, strSizes,
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }

    public void printSkyline(String materTableName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException, Catalogrelexists, Catalogmissparam, Catalognomem, RelCatalogException, Cataloghferror, Catalogdupattrs, Catalogioerror, FileAlreadyDeletedException {
        Heapfile file = null;
        attrInfo[] attrs = new attrInfo[noOfColumns];
        if (checkToMaterialize(materTableName)) {
            int SIZE_OF_INT = 4;
            for (int i = 0; i < noOfColumns; ++i) {
                attrs[i] = new attrInfo();
                attrs[i].attrType = new AttrType(in1[i].attrType);
                attrs[i].attrName = "Col" + i;
                attrs[i].attrLen = (in1[i].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 32;
            }
            file = new Heapfile(materTableName);
        }
        int count = 0;

        Tuple tt = new Tuple();
        try {
            tt.setHdr(noOfColumns, in1, strSizes);
        } catch (Exception e) {
            e.printStackTrace();
        }

        int size = tt.size();

        Tuple t = new Tuple(size);
        try {
            t.setHdr(noOfColumns, in1, strSizes);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert skylineItr != null;

        try {
            tt = skylineItr.get_next();
            while (tt != null) {
                t.tupleCopy(tt);
                if (checkToMaterialize(materTableName)) {
                    file.insertRecord(t.returnTupleByteArray());
                } else {
                    t.print(in1);
                }
                count++;
                tt = skylineItr.get_next();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Skyline computation completed!");
        System.out.println("No of skyline members -> " + count);
        if (checkToMaterialize(materTableName)) {
            try {
                ExtendedSystemDefs.MINIBASE_RELCAT.createRel(materTableName, noOfColumns, attrs);
            } catch (Exception e) {
                System.err.println("Error occurred while creating materialized view!");
                file.deleteFile();
            }
            System.out.println("Created materialize view! -> " + materTableName);
        }
        System.out.println("---------------------------------------------------------------------------------------------------------");
    }

    private boolean checkToMaterialize(String materTableName) {
        return (materTableName != null && materTableName.length() > 0);
    }

    /**
     * Gets the next tuple in the skyline heap file
     *
     * @return                              tuple in skyline
     * @throws IOException                  exception from this class
     * @throws JoinsException               exception from this class
     * @throws IndexException               exception from this class
     * @throws InvalidTupleSizeException    exception from this class
     * @throws InvalidTypeException         exception from this class
     * @throws PageNotReadException         exception from this class
     * @throws TupleUtilsException          exception from this class
     * @throws PredEvalException            exception from this class
     * @throws SortException                exception from this class
     * @throws LowMemException              exception from this class
     * @throws UnknowAttrType               exception from this class
     * @throws UnknownKeyTypeException      exception from this class
     * @throws Exception                    exception from this class
     */
    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if( skylineItr != null ) {
            TupleRIDPair currTupleRid = skylineItr.get_next1();
            if(currTupleRid == null) {
                // Deleting records in temp heap file after scan is completed
                for(RID skyRid : skylineRIDList) {
                    skyFile.deleteRecord(skyRid);
                }
                skylineRIDList = Collections.EMPTY_LIST;
                return null;
            }
            Tuple currTuple = currTupleRid.getTuple();
            RID currRid = currTupleRid.getRID();
            skylineRIDList.add(currRid);
            return currTuple;
        }
        return null;
    }

    /**
     * Cleaning up, including releasing buffer pages from the buffer pool
     * and removing temporary files from the database.
     *
     * @throws IOException      exception from this class
     * @throws JoinsException   exception from this class
     * @throws SortException    exception from this class
     * @throws IndexException   exception from this class
     */
    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {
            try {
                if(skylineItr != null) {
                    skylineItr.close();
                }
                if(skylineItr != null) {
                    skyFile.deleteFile();
                }
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}

