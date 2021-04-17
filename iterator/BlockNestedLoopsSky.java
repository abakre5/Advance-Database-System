package iterator;

import catalog.*;
import global.AttrType;
import global.ExtendedSystemDefs;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author Abhishek Bakare
 */
public class BlockNestedLoopsSky extends Iterator {

    private final AttrType[] attrTypes;
    private final short noOfColumns;
    private final int noOfBufferPages;
    private final Iterator outer;
    private final int[] prefList;
    private final int prefListLength;
    private final List<Tuple> skyline;
    private final short[] stringSizes;
    private final String relationName;
    private ArrayList<Tuple> window;
    private int windowSize;
    private String tempFileName;


    /**
     * Block Nested Loop Skyline Constructor: assign passed values to the private variable used in BlockNestedLoopSky
     * and then start the computation of skyline members
     *
     * @param in1
     * @param len_in1
     * @param t1_str_sizes
     * @param am1
     * @param relationName
     * @param pref_list
     * @param pref_list_length
     * @param n_pages
     * @throws Exception
     */
    public BlockNestedLoopsSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1, String relationName,
                               int[] pref_list, int pref_list_length, int n_pages) throws Exception {
        this.attrTypes = in1;
        this.noOfBufferPages = n_pages;
        this.outer = am1;
        this.prefList = pref_list;
        this.prefListLength = pref_list_length;
        this.stringSizes = t1_str_sizes;
        this.noOfColumns = (short) len_in1;
        this.relationName = relationName;
        this.window = new ArrayList<>();
        this.skyline = new ArrayList<>();
        this.windowSize = -1;
        this.tempFileName = "temp_file";
        computeBlockNestedSkyline();
    }

    /**
     * Compute Window Size
     * Iterate over the data iterator and compare each element in it with an element in the Window
     * If the member is probable to be the part of skyline, then insert it into skyline(Window/Temp_File)
     * At end of each iteration elements in the window are declared as skyline.
     * If any elements are present in the disk, then pass heap file for vetting.
     *
     * @throws Exception
     */
    private void computeBlockNestedSkyline() throws Exception {
        Tuple tuple = outer.get_next();
        Heapfile disk1 = null;
        if (windowSize == -1) {
            windowSize = (int) Math.floor(Tuple.MINIBASE_PAGESIZE / (int) tuple.size() * noOfBufferPages);
        }
        disk1 = parseDiskFile(tuple, tempFileName, outer);
        skyline.addAll(window);
        if (disk1 != null) {
            vetDiskSkylineMembers(tempFileName, 0);
        }
    }

    /**
     * Parse dataset/file for computing skyline members of it
     *
     * @param tuple
     * @param tempFileName
     * @param outer
     * @return disk on which un-vetted skyline members are written
     * @throws Exception
     */
    private Heapfile parseDiskFile(Tuple tuple, String tempFileName, Iterator outer) throws Exception {
        boolean flag = true;
        Heapfile disk = null;
        while (tuple != null) {
            Tuple currentTuple = new Tuple(tuple);
            boolean isMemberOfSkyline = compareTupleWithWindowForDominance(currentTuple);
            if (isMemberOfSkyline) {
                if (window.size() < windowSize && flag) {
                    window.add(currentTuple);
                } else {
                    if (disk == null) {
                        flag = false;
                        disk = getHeapFileInstance(tempFileName);
                    }
                    disk.insertRecord(currentTuple.returnTupleByteArray());
                }
            }
            tuple = outer.get_next();
        }

        return disk;
    }

    /**
     * Compare itrTuple with every element in the window.
     *
     * @param itrTuple
     * @return
     * @throws IOException
     * @throws TupleUtilsException
     */
    private boolean compareTupleWithWindowForDominance(Tuple itrTuple) throws IOException, TupleUtilsException, UnknowAttrType {
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

    /**
     * Get Instance of the Heapfile
     *
     * @param fileName
     * @return
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     */
    private Heapfile getHeapFileInstance(String fileName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        return new Heapfile(fileName);
    }

    /**
     * Vet elements in the temp_file similar to the @computeBlockNestedSkyline method.
     * If the window size becomes full in between of the iteration, the elements are added into the heapfile
     * and again the heapfile is passed for vetting.
     *
     * @param relationName
     * @param i
     * @throws Exception
     */
    private void vetDiskSkylineMembers(String relationName, int i) throws Exception {
        Heapfile temp = null;
        String temp_file_name = tempFileName + i;
        FileScan diskOuterScan = getFileScan(relationName);
        Tuple tuple = diskOuterScan.get_next();
        if (tuple != null) {
            window.clear();
            temp = parseDiskFile(tuple, temp_file_name, diskOuterScan);
            diskOuterScan.close();
            diskOuterScan.deleteFile();
            skyline.addAll(window);
            i++;
            if (temp != null) {
                vetDiskSkylineMembers(temp_file_name, i);
            }
        }
    }

    /**
     * Initializes file scan on the relation
     *
     * @param relationName
     * @return
     * @throws IOException
     * @throws FileScanException
     * @throws TupleUtilsException
     * @throws InvalidRelation
     */
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

    /**
     * This method output each element declared as skyline member one by one.
     *
     * @return
     */
    @Override
    public Tuple get_next() {
        if (!skyline.isEmpty()) {
            Tuple nextTuple = skyline.get(0);
            skyline.remove(0);
            return nextTuple;
        }
        return null;
    }

    public List<Tuple> getAllSkylineMembers() {
        return this.skyline;
    }

    /**
     * close the initialItr
     *
     * @throws IOException
     * @throws JoinsException
     * @throws SortException
     * @throws IndexException
     */
    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        outer.close();
    }

    public void printSkyline(String materTableName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException, Catalogrelexists, Catalogmissparam, Catalognomem, RelCatalogException, Cataloghferror, Catalogdupattrs, Catalogioerror, FileAlreadyDeletedException {
        Heapfile file = null;
        attrInfo[] attrs = new attrInfo[noOfColumns];
        if (checkToMaterialize(materTableName)) {
            int SIZE_OF_INT = 4;
            for (int i = 0; i < noOfColumns; ++i) {
                attrs[i] = new attrInfo();
                attrs[i].attrType = new AttrType(attrTypes[i].attrType);
                attrs[i].attrName = "Col" + i;
                attrs[i].attrLen = (attrTypes[i].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 32;
            }
            file = new Heapfile(materTableName);
        }
        int count = 0;
        for (Tuple tuple : skyline) {
            if (checkToMaterialize(materTableName)) {
                file.insertRecord(tuple.returnTupleByteArray());
            } else {
                tuple.print(attrTypes);
            }
            count++;
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
}
