package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BlockNestedLoopsSky extends Iterator {

    private final AttrType[] attrTypes;
    private final short noOfColumns;
    private final int noOfBufferPages;
    private final Iterator outer;
    private final int[] prefList;
    private final int prefListLength;
    private final List<Tuple> skyline;
    private final short[] stringSizes;
    private ArrayList<Tuple> window;
    private Heapfile disk = null;
    private int noOfDiskElements;
    private int windowSize;

    public BlockNestedLoopsSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, String relationName,
                               int[] pref_list, int pref_list_length, int n_pages) throws Exception {
        this.attrTypes = in1;
        this.noOfBufferPages = n_pages;
        this.outer = am1;
        this.prefList = pref_list;
        this.prefListLength = pref_list_length;
        this.stringSizes = t1_str_sizes;
        this.noOfColumns = (short) len_in1;

        this.window = new ArrayList<>();
        this.skyline = new ArrayList<>();
        this.noOfDiskElements = 0;
        this.windowSize = -1;
        computeBlockNestedSkyline();
    }

    private void computeBlockNestedSkyline() throws Exception {
        Tuple tuple = outer.get_next();
        if (windowSize == -1) {
            windowSize = (int) Math.floor(Tuple.MINIBASE_PAGESIZE * noOfBufferPages / (int) tuple.size());
        }
        do {
            Tuple currentTuple = new Tuple(tuple);
            boolean isMemberOfSkyline = compareTupleWithWindowForDominance(currentTuple);
            if (isMemberOfSkyline) {
                insertIntoSkyline(currentTuple);
            }
            tuple = outer.get_next();
        } while (tuple != null);
        skyline.addAll(window);
        vetDiskSkylineMembers();
    }

    private boolean compareTupleWithWindowForDominance(Tuple itrTuple) throws IOException, TupleUtilsException {
        boolean isDominating = true;
        ArrayList<Tuple> elementsNotBelongingToWindow = new ArrayList<>();
        for (Tuple tupleInWindow : window) {
            if (TupleUtils.Dominates(tupleInWindow, attrTypes, itrTuple, attrTypes, noOfColumns, stringSizes, prefList, prefListLength)) {
                isDominating = false;
                break;
            } else if (TupleUtils.Dominates(itrTuple, attrTypes, tupleInWindow, attrTypes, noOfColumns, stringSizes, prefList, prefListLength)) {
                elementsNotBelongingToWindow.add(tupleInWindow);
            }
        }
        window.removeAll(elementsNotBelongingToWindow);
        return isDominating;
    }

    private Heapfile getHeapFileInstance() throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        String relationName = "disk.in";
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

    private void vetDiskSkylineMembers() throws Exception {
        if (disk != null && noOfDiskElements > 0) {
            window.clear();
            HashSet<RID> nonDominatingTuples = new HashSet<>();
            FileScan diskOuterScan = getFileScan();
            TupleRIDPair tupleRIDPairOuter = diskOuterScan.get_next1();
            while (tupleRIDPairOuter != null) {
                Tuple tupleOuter = tupleRIDPairOuter.getTuple();
                RID ridOuter = tupleRIDPairOuter.getRID();
                Tuple diskTupleToCompare = new Tuple(tupleOuter);
                if (window.size() < windowSize) {
                    boolean isMemberOfSkyline = compareTupleWithWindowForDominance(diskTupleToCompare);
                    if (isMemberOfSkyline) {
                        window.add(diskTupleToCompare);
                    }
                    nonDominatingTuples.add(ridOuter);
                } else {
                    break;
                }
                tupleRIDPairOuter = diskOuterScan.get_next1();
            }
            for (RID ridToDelete : nonDominatingTuples) {
                disk.deleteRecord(ridToDelete);
                noOfDiskElements--;
            }
            diskOuterScan.close();
            skyline.addAll(window);
            vetDiskSkylineMembers();
        }
    }

    private FileScan getFileScan() throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;
        String relationName = "disk.in";

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, stringSizes,
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }

    private boolean checkDiskSkylineMemberIsDominating(Tuple diskTuple) throws Exception {
        for (Tuple currentTuple : skyline) {
            if (TupleUtils.Dominates(currentTuple, attrTypes, diskTuple, attrTypes, noOfColumns, stringSizes, prefList, prefListLength)) {
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
