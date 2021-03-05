package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
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
        computeSortSkyline();
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
        do {
            tupleList.add(new Tuple(tuple));
            tuple = outer.get_next();
        } while (tuple != null);
        for (Tuple currentTuple : tupleList) {
            if (isSkylineCandidate(currentTuple)) {
                skyline.add(currentTuple);
            }
        }
//        System.out.println(skyline);
    }

    private boolean isSkylineCandidate(Tuple currTuple) throws IOException, TupleUtilsException {
        for (Tuple tupleInSkyline : skyline) {
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
