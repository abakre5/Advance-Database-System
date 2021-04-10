package iterator;

import bufmgr.PageNotReadException;
import diskmgr.PageCounter;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the Sorted First skyline computation algorithm.
 * @author Manthan Agrawal
 */
public class SortFirstSky extends Iterator {

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
    private int windowSize;
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
        this.window = new ArrayList<>();
        this.windowSize = -1;
        computeSortSkyline();
    }

    /**
     * This methods handles the pre-processing of the data for the sorted first method.
     * checked if data is sorted or not of not passes the data to sort method.
     * finally for each tuple isSkylineCandidate() method is called to check if the candidate is skyline tuple.
     *
     * @throws Exception - file scan exception.
     */
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
            System.out.println("Read counter before sort :" +PageCounter.getReadCounter());
            System.out.println("write counter before sort :" + PageCounter.getWriteCounter());
            System.out.println("Data is not sorted ... Sorting data ...");
            outer = sortData(fscan);
            System.out.println("Read counter after sort :" +PageCounter.getReadCounter());
            System.out.println("write counter after sort :" + PageCounter.getWriteCounter());
        } else {
            System.out.println("Data is already sorted.");
            outer = fscan;
        }

        Tuple tuple = outer.get_next();
        if (windowSize == -1) {
//           windowSize = noOfBufferPages;
            windowSize = (int) Math.floor((Tuple.MINIBASE_PAGESIZE / (int) tuple.size() * noOfBufferPages));
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

    /**
     * This method checks if the given data is in order is in sorted order or not.
     * This methods checks for data is in ascending order.
     * This methods uses CompareTupleWithTuplePref() which uses sum of preference attributes to compare two tuples.
     *
     * @return if data file is in ascending order then return - true else false.
     * @throws Exception - Exception for iteration operations.
     */
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

    /**
     * This methods calls the SortPref() to sort the data on the given file Scan.
     * It calls for the ascending order of the data as min Skyline is computed in our case.
     *
     * @param fscan - file scan on which sort needs to be performed.
     * @return - Iterator of the sorted data file.
     */
    private Iterator sortData(FileScan fscan) {
        SortPref sort = null;
        try {
            sort = new SortPref(attrTypes, noOfColumns, stringSizes, fscan, new TupleOrder(TupleOrder.Ascending), prefList, prefList.length, noOfBufferPages);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sort;
    }

    /**
     * Method Initiates the filescan on the given file Name.
     *
     * @param relationName - File name on which scan needs to be initiated.
     * @return - file scan created.
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
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

    /**
     * Vet elements in the temp_file similar to the @isSkylineCandidate method.
     * If the window size becomes full in between of the iteration, the elements are added into the heapfile
     * and again the heapfile is passed for vetting.
     *
     * @param relationName - name of file which contains not vetted tuples
     * @param i            - incremental counter to avoid same file name.
     */
    private void vetDiskSkylineMembers(String relationName, int i) throws Exception {
        Heapfile temp = null;
        String temp_file_name = tempFileName + i;
        FileScan diskOuterScan = getFileScan(relationName);
        TupleRIDPair tupleRIDPairOuter = diskOuterScan.get_next1();
        if (tupleRIDPairOuter != null) {
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

    /**
     * @param relationName - name on of heap file which needs to be created.
     * @return - heap file.
     * @throws HFException        heap file exception
     * @throws HFBufMgrException  exception thrown from bufmgr layer
     * @throws HFDiskMgrException exception thrown from diskmgr layer
     * @throws IOException        I/O errors
     */
    private Heapfile getHeapFileInstance(String relationName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        return new Heapfile(relationName);
    }

    /**
     * This methods implemets logic to added tuple, if window has space available then tuple will be added to window
     * otherwise it will add tuple to given heap File.
     *
     * @param heapfile     heap file in which data needs to be added.
     * @param currentTuple tuple which will be added to window/heapfile.
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws SpaceNotAvailableException no space left
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    private void insertIntoSkyline(Heapfile heapfile, Tuple currentTuple) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, SpaceNotAvailableException, InvalidSlotNumberException {
        if (window.size() < windowSize) {
            window.add(currentTuple);
        } else {
            heapfile.insertRecord(currentTuple.returnTupleByteArray());
        }
    }

    /**
     * This method compares the given tuple to all the tuples in the window.
     * if any tuple in window dominates the currTuple then false value is returned.
     * @param currTuple - Tuple which needs to be compared against the window.
     * @return - true if no tuple in window dominates given tuple.
     */
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
