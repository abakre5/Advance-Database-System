package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
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
    private String tempFileName = "skyFirstSkylinesTemp";
    boolean flag = true;




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
        Heapfile disk1 = null;
        boolean isSorted = checkIfDataIsSorted();
        FileScan fscan = null;
        try {
            fscan = getFileScan(relationName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!isSorted) {
            System.out.println("Data is not sorted Sorting data.");
            outer = sortData(fscan);
        } else {
            System.out.println("Data is already sorted.");
            outer = fscan;
        }

        Tuple tuple = outer.get_next();
        if (windowSize == -1) {
//           windowSize = noOfBufferPages;
            windowSize = (int) Math.floor(Tuple.MINIBASE_PAGESIZE / (int) tuple.size() *noOfBufferPages);
        }
        while (tuple != null) {
            Tuple currentTuple = new Tuple(tuple);
            if (isSkylineCandidate(currentTuple)) {
                if (window.size() == windowSize) {
                    disk1 = getHeapFileInstance(tempFileName);
                }
                insertIntoSkyline(disk1, currentTuple);
            }
            tuple = outer.get_next();
        }
        skyline.addAll(window);
        vetDiskSkylineMembers(tempFileName, 0);
    }

    private boolean checkIfDataIsSorted() throws Exception {
        Tuple curr = outer.get_next();
        while (curr != null) {
            curr = new Tuple(curr);
            Tuple next = outer.get_next();
            if (next != null) {
                next = new Tuple(next);
                int pref = TupleUtils.CompareTupleWithTuplePref(curr, attrTypes, next, attrTypes, noOfColumns, stringSizes, prefList, prefList.length);
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
            sort = new SortPref(attrTypes, noOfColumns, stringSizes, fscan, new TupleOrder(TupleOrder.Ascending), prefList, prefList.length, noOfBufferPages);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        Tuple t = new Tuple(sort.get_next());
//        System.out.println(t.getIntFld(1));
        return sort;
    }




    private FileScan getFileScan(String relationName) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;
        FldSpec[] pProjection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            pProjection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, stringSizes,
                noOfColumns, noOfColumns, pProjection, null);
        return scan;
    }


    private void vetDiskSkylineMembers(String relationName, int i) throws Exception {
        Heapfile temp = null;
        String temp_file_name = tempFileName + i;
        FileScan diskOuterScan = getFileScan(relationName);
        TupleRIDPair tupleRIDPairOuter = diskOuterScan.get_next1();
        if (tupleRIDPairOuter!= null) {
            window.clear();
            while (tupleRIDPairOuter != null) {
                Tuple tupleOuter = tupleRIDPairOuter.getTuple();
                RID ridOuter = tupleRIDPairOuter.getRID();
                Tuple diskTupleToCompare = new Tuple(tupleOuter);
                boolean isMemberOfSkyline = isSkylineCandidate(diskTupleToCompare);
                if (isMemberOfSkyline) {
                    if (window.size() < windowSize) {
                        window.add(diskTupleToCompare);
                    } else {
                        if (temp == null) {
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
            vetDiskSkylineMembers(temp_file_name, i);
        }
    }

    private Heapfile getHeapFileInstance(String relationName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        return new Heapfile(relationName);
    }

    private void insertIntoSkyline(Heapfile heapfile, Tuple currentTuple) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, InvalidTypeException, SpaceNotAvailableException, InvalidSlotNumberException, FileScanException, TupleUtilsException, InvalidRelation {
        if (window.size() < windowSize) {
            window.add(currentTuple);
        } else {
            heapfile.insertRecord(currentTuple.returnTupleByteArray());
        }
    }

    private boolean isSkylineCandidate(Tuple currTuple) throws IOException, TupleUtilsException, FieldNumberOutOfBoundException {
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
