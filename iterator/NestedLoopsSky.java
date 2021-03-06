package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created on 2/21/2021
 * @author Rahul
 */
public class NestedLoopsSky extends Iterator {
    private AttrType[] in1;
    private int n_buf_pgs;
    private Iterator outer;
    private int[] pref_list;
    private List<Tuple> skyline;
    private boolean first_time;
    private String relation_name;
    private short noOfColumns;
    private int noOfBufferPages;

    public NestedLoopsSky(AttrType[] _in1,
                          int amt_of_mem,
                          Iterator am1,
                          int[] _pref_list
    ) throws IOException,NestedLoopException {
        in1 = _in1;
        n_buf_pgs = amt_of_mem;
        outer = am1;
        pref_list = _pref_list;
        first_time = true;
    }

    public NestedLoopsSky(AttrType[] _in1,
                          int len_in1,
                          Iterator am1,
                          String _relationName,
                          int[] _pref_list,
                          int n_pages
    ) throws Exception {
        in1 = _in1;
        noOfColumns = (short) len_in1;
        outer = am1;
        pref_list = _pref_list;
        relation_name = _relationName;
        noOfBufferPages = n_pages;
//        first_time = true;
        setSkyline();
    }

    /**
     * Steps for performing getSkyline:
     * (1) Open file mentioned by relation name
     * (2) Have 2 scan:- inner and outer loop on file
     * (3) get_next, Compare, if one is dominated, delete that record
     */
    private void setSkyline() throws Exception {
        if(!relation_name.isEmpty()) {
            // We use outer as outer loop
            // Creating new scan for inner loop
            Heapfile f = null;
            try {
                f = new Heapfile(relation_name);
            } catch (Exception e) {
                System.err.println(" Could not open heapfile");
                e.printStackTrace();
            }

            FileScan outerScan = getFileScan();
            FileScan innerScan = getFileScan();

            TupleRIDPair outerTupleRid = outerScan.get_next1();
            while(outerTupleRid != null) {
                TupleRIDPair innerTupleRid = innerScan.get_next1();
                while(innerTupleRid != null) {
                    Tuple outerTuple = outerTupleRid.getTuple();
                    Tuple innerTuple = innerTupleRid.getTuple();
                    RID innerRid = innerTupleRid.getRID();
                    if( TupleUtils.Dominates( outerTuple, in1, innerTuple, in1, (short)in1.length, new short[0], pref_list, pref_list.length ) ) {
                        try {
                            f.deleteRecord(innerRid);
                        } catch (Exception e) {
                            System.err.println("*** Error deleting record \n");
                            e.printStackTrace();
                            break;
                        }
                    }
                    innerTupleRid = innerScan.get_next1();
                }
                innerScan.close();
                innerScan = getFileScan();
                outerTupleRid = outerScan.get_next1();
            }
            outerScan.close();

            // For now, scan and store in skyline ArrayList
            outer = getFileScan();
        } else {
            System.out.println("ERROR: Relation name not specified");
        }
    }

    private FileScan getFileScan() throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan;

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relation_name, in1, new short[0],
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        TupleRIDPair currTupleRid = outer.get_next1();
        if(currTupleRid == null) {
            outer.close();
            return null;
        }
        Tuple currTuple = currTupleRid.getTuple();
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

