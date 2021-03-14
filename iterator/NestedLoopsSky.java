package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.*;

/**
 * Created on 2/21/2021
 * @author Rahul
 */
public class NestedLoopsSky extends Iterator {
    private AttrType[] in1;
    private int n_buf_pgs;
    private Iterator outer;
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
        setSkyline();
    }

    /**
     * Steps for performing getSkyline:
     * (1) Open file mentioned by relation name
     * (2) Have 2 scan:- inner and outer loop on file
     * (3) get_next, Compare, if one is dominated, delete that record
     */
    private void setSkyline() throws Exception {
        if(!relationName.isEmpty()) {
            try {
                skyFile = new Heapfile("skynls.in");
            } catch (Exception e) {
                System.err.println(" Could not open heapfile");
                e.printStackTrace();
            }

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
            outer = getFileScan("skynls.in");
        } else {
            System.out.println("ERROR: Relation name not specified");
        }
    }

    private boolean containsRid(Set<RID> ridSet, RID currRid) {
        for( RID rid : ridSet ) {
            if ( currRid.equals(rid) ) {
                return true;
            }
        }
        return false;
    }

    private FileScan getFileScan(String relation) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan;

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relation, in1, new short[0],
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        TupleRIDPair currTupleRid = outer.get_next1();
        if(currTupleRid == null) {
            // Deleting records in temp heap file after scan is completed
            for(RID skyRid : skylineRIDList) {
                skyFile.deleteRecord(skyRid);
            }
            skylineRIDList = Collections.EMPTY_LIST;
            outer.close();
            return null;
        }
        Tuple currTuple = currTupleRid.getTuple();
        RID currRid = currTupleRid.getRID();
        skylineRIDList.add(currRid);
        return currTuple;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {
            try {
                outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}

