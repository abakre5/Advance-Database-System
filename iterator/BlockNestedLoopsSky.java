package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BlockNestedLoopsSky extends Iterator {

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
    private ArrayList<Tuple> diskMemory;

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
        this.diskMemory = new ArrayList<>();
        this.skyline = new ArrayList<>();
        this.tupleList = new ArrayList<>();
        computeBlockNestedSkyline();
    }

    private void computeBlockNestedSkyline() throws Exception {
        Tuple tuple = outer.get_next();
        do {
            tupleList.add(new Tuple(tuple));
            tuple = outer.get_next();
        } while (tuple != null);
        for (Tuple currentTuple : tupleList) {
            boolean isMemberOfSkyline = compareTupleWithWindowForDominance(currentTuple);
            if (isMemberOfSkyline) {
                insertIntoSkyline(currentTuple);
            }
        }
        skyline.addAll(window);
        vetDiskSkylineMembers();
    }

    private boolean compareTupleWithWindowForDominance(Tuple itrTuple) throws IOException, TupleUtilsException {
        boolean isDominating = true;
        Tuple nonDominatingTupleInRelation = null;
        ArrayList<Tuple> nonDominatingTuplesInWindow = new ArrayList<>();
        for (Tuple tupleInWindow : window) {
            if (TupleUtils.Dominates(tupleInWindow, attrTypes, itrTuple, attrTypes, noOfColumns, stringSizes, prefList, prefListLength)) {
                isDominating = false;
                nonDominatingTupleInRelation = itrTuple;
                break;
            } else if (TupleUtils.Dominates(itrTuple, attrTypes, tupleInWindow, attrTypes, noOfColumns, stringSizes, prefList, prefListLength)) {
                nonDominatingTuplesInWindow.add(tupleInWindow);
            }
        }
        if (!isDominating) {
            tupleList.remove(nonDominatingTupleInRelation);
        }
        window.removeAll(nonDominatingTuplesInWindow);
        return isDominating;
    }

    private void insertIntoSkyline(Tuple currentTuple) {
        if (window.size() < noOfBufferPages) {
            window.add(currentTuple);
        } else {
            diskMemory.add(currentTuple);
        }
    }

    private void vetDiskSkylineMembers() throws Exception {
        if (!diskMemory.isEmpty()) {
            tupleList.removeAll(window);
            window.clear();
            ArrayList<Tuple> nonDominatingTuples = new ArrayList<>();
            for (Tuple diskTuple : diskMemory) {
                if (window.size() < noOfBufferPages) {
                    boolean isDominating = checkDiskSkylineMemberIsDominating(diskTuple);
                    if (isDominating) {
                        window.add(diskTuple);
                    }
                    nonDominatingTuples.add(diskTuple);
                }
            }
            diskMemory.removeAll(nonDominatingTuples);
            skyline.addAll(window);
            vetDiskSkylineMembers();
        }
    }

    private boolean checkDiskSkylineMemberIsDominating(Tuple diskTuple) throws Exception {
        for (Tuple currentTuple : tupleList) {
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
