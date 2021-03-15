package iterator;

import diskmgr.PageCounter;
import global.AttrType;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
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
    private int windowSize;
    private String tempFileName;


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
        this.windowSize = -1;
        this.tempFileName = "temp_file";
        computeBlockNestedSkyline();
    }

    private void computeBlockNestedSkyline() throws Exception {
        Tuple tuple = outer.get_next();
        Heapfile disk1 = null;
        if (windowSize == -1) {
            windowSize = (int) Math.floor(Tuple.MINIBASE_PAGESIZE / (int) tuple.size() * noOfBufferPages );
        }
        do {
            Tuple currentTuple = new Tuple(tuple);
            boolean isMemberOfSkyline = compareTupleWithWindowForDominance(currentTuple);
            if (isMemberOfSkyline) {
                if (window.size() == windowSize && disk1 == null) {
                    disk1 = getHeapFileInstance(tempFileName);
                }
                insertIntoSkyline(disk1, currentTuple);
            }
            tuple = outer.get_next();
        } while (tuple != null);
        skyline.addAll(window);
        if (disk1 != null) {
            vetDiskSkylineMembers(tempFileName, 0);
        }
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

    private Heapfile getHeapFileInstance(String fileName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        return new Heapfile(fileName);
    }

    private void insertIntoSkyline(Heapfile heapfile, Tuple currentTuple) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, InvalidTypeException, SpaceNotAvailableException, InvalidSlotNumberException, FileScanException, TupleUtilsException, InvalidRelation {
        if (window.size() < windowSize) {
            window.add(currentTuple);
        } else {
            heapfile.insertRecord(currentTuple.returnTupleByteArray());
        }
    }

    private void vetDiskSkylineMembers(String relationName, int i) throws Exception {
        Heapfile temp = null;
        String temp_file_name = tempFileName + i;
        FileScan diskOuterScan = getFileScan(relationName);
        TupleRIDPair tupleRIDPairOuter = diskOuterScan.get_next1();
        if (tupleRIDPairOuter!= null) {
            window.clear();
            boolean flag = true;
            while (tupleRIDPairOuter != null) {
                Tuple tupleOuter = tupleRIDPairOuter.getTuple();
                Tuple diskTupleToCompare = new Tuple(tupleOuter);
                boolean isMemberOfSkyline = compareTupleWithWindowForDominance(diskTupleToCompare);
                if (isMemberOfSkyline) {
                    if (window.size() < windowSize && flag) {
                           window.add(diskTupleToCompare);
                    } else {
                        if (temp == null) {
                            flag = false;
                            temp = getHeapFileInstance(temp_file_name);
                        }
                        temp.insertRecord(diskTupleToCompare.returnTupleByteArray());
                    }
                }
                tupleRIDPairOuter = diskOuterScan.get_next1();
            }
            diskOuterScan.close();
            diskOuterScan.deleteFile();
            skyline.addAll(window);
            i++;
            if (temp != null) {
                vetDiskSkylineMembers(temp_file_name, i);
            }
        }
    }

    private FileScan getFileScan(String relationName) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, stringSizes,
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }

    @Override
    public Tuple get_next(){
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
