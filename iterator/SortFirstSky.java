package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SortFirstSky extends Iterator {

    private ArrayList<Tuple> tupleList;
    private AttrType[] attrTypes;
    private short noOfColumns;
    private int noOfBufferPages;
    private Iterator outer;
    private int[] prefList;
    private int prefListLength;
    private List<Tuple> skyline;
    private short[] stringSizes;
    private String relationName;
    private ArrayList<Tuple> window;
    private Heapfile disk = null;
    private int windowSize;
    private int noOfDiskElements;



    public SortFirstSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, String relationName,
                        int[] pref_list, int pref_list_length, int n_pages) throws Exception {
        this.attrTypes = in1;
        this.noOfBufferPages = n_pages;
        this.outer = am1;
        this.prefList = pref_list;
        this.prefListLength = pref_list_length;
        this.stringSizes = t1_str_sizes;
        this.noOfColumns = (short) len_in1;
        this.relationName = relationName;
        this.skyline = new ArrayList<>();
        this.tupleList = new ArrayList<>();
        this.window = new ArrayList<>();
        this.windowSize = -1;
        this.noOfDiskElements = 0;
        computeSortSkyline();
    }

    private void computeSortSkyline() throws Exception {
        boolean isSorted = checkIfDataIsSorted();
        FileScan fscan = null;
        FldSpec[] projList = new FldSpec[noOfColumns];
        int i = 0;
        while (i < noOfColumns) {
            projList[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            i++;
        }
        try {
            fscan = new FileScan(relationName, attrTypes, stringSizes, noOfColumns, noOfColumns, projList, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!isSorted) {
            System.out.println("Data is not sorted sorting data now.");
            outer = sortData(fscan);
        } else {
            System.out.println("Data is sorted directly computing skyline.");
            outer = fscan;
        }

        Tuple tuple = outer.get_next();
        if (windowSize == -1) {
            windowSize = (int) Math.floor(Tuple.MINIBASE_PAGESIZE * noOfBufferPages / (int) tuple.size());
        }
        do {
            Tuple currentTuple = new Tuple(tuple);
            if (isSkylineCandidate(currentTuple)) {
                System.out.println(tuple.getIntFld(1));
                insertIntoSkyline(currentTuple);
            }
            tuple = outer.get_next();
        } while (tuple != null);
        skyline.addAll(window);
        vetDiskSkylineMembers();
    }

    private boolean checkIfDataIsSorted() throws Exception {
        Tuple curr = outer.get_next();
        while (curr != null) {
            curr = new Tuple(curr);
            Tuple next = outer.get_next();
            if (next != null) {
                next = new Tuple(next);
                int pref = TupleUtils.CompareTupleWithTuplePref(next, attrTypes, curr, attrTypes, noOfColumns, stringSizes, prefList, prefList.length);
                if (pref == 1) {
                    return false;
                }
            }
            curr = next;
        }
        return true;
    }

    private Iterator sortData(FileScan fscan) throws Exception {
        SortPref sort = null;
        try {
            sort = new SortPref(attrTypes, noOfColumns, stringSizes, fscan, new TupleOrder(TupleOrder.Descending), prefList, prefList.length, noOfBufferPages);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        Tuple t = new Tuple(sort.get_next());
//        System.out.println(t.getIntFld(1));
        return sort;
    }




    private FileScan getFileScan() throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;
        String relationName = "sortFirstDisk.in";

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, stringSizes,
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }


    private void vetDiskSkylineMembers() throws Exception {
        if (disk != null && noOfDiskElements > 0) {
            window.clear();
            HashSet<RID> vettedTuples = new HashSet<>();
            FileScan diskOuterScan = getFileScan();
            TupleRIDPair tupleRIDPairOuter = diskOuterScan.get_next1();
            while (tupleRIDPairOuter != null) {
                Tuple tupleOuter = tupleRIDPairOuter.getTuple();
                RID ridOuter = tupleRIDPairOuter.getRID();
                Tuple diskTupleToCompare = new Tuple(tupleOuter);
                if (window.size() < windowSize) {
                    if (isSkylineCandidate(diskTupleToCompare)) {
                        window.add(diskTupleToCompare);
                    }
                    vettedTuples.add(ridOuter);
                } else {
                    break;
                }
                tupleRIDPairOuter = diskOuterScan.get_next1();
            }
            for (RID ridToDelete : vettedTuples) {
                disk.deleteRecord(ridToDelete);
                noOfDiskElements--;
            }
            diskOuterScan.close();
            skyline.addAll(window);
            vetDiskSkylineMembers();
        }
    }
    private Heapfile getHeapFileInstance() throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        String relationName = "sortFirstDisk.in";
        return new Heapfile(relationName);
    }

    private void insertIntoSkyline(Tuple currentTuple) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, InvalidTypeException, SpaceNotAvailableException, InvalidSlotNumberException {
        if (window.size() < windowSize) {
            window.add(currentTuple);
        } else {
            if (disk == null) {
                disk = getHeapFileInstance();
            }
            disk.insertRecord(currentTuple.returnTupleByteArray());
            noOfDiskElements++;
        }
    }

    private boolean isSkylineCandidate(Tuple currTuple) throws IOException, TupleUtilsException {
        for (Tuple tupleInSkyline : window) {
            if (TupleUtils.Dominates(tupleInSkyline, attrTypes, currTuple, attrTypes, noOfColumns, stringSizes, prefList, prefListLength)) {
                return false;
            }
        }
        return true;
    }


    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if (!skyline.isEmpty()) {
            Tuple nextTuple = skyline.get(0);
            skyline.remove(0);
            return nextTuple;
        }
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        outer.close();
    }

}
