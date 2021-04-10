package iterator;

import bufmgr.PageNotReadException;
import global.AggType;
import global.AttrType;
import global.TupleOrder;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Abhishek Bakare
 */
public class GroupBywithSort {

    private final AttrType[] attrType;
    private final short noOfColumns;
    private final short[] strSize;
    private final Iterator itr;
    private final FldSpec groupByAttr;
    private final FldSpec[] aggList;
    private final AggType aggType;
    private final FldSpec[] projList;
    private final int nOutFields;
    private final int nPages;

    private static final String SortedRelationName = "SortedTuples";

    private Heapfile sortedTuples;
    private FileScan scan;

    /**
     * Perform Group by operation on a relation using @Sort class of the Minibase.
     *
     * @param in1           Attribute Types in the relation
     * @param len_in1       No Of Columns in the relation
     * @param t1_str_sizes  String size in the relation if any
     * @param am1           Filescan over the relation
     * @param group_by_attr Attribute on which group by needs to be performed
     * @param agg_list      Aggregation Attributes
     * @param agg_type      Aggregation Attributes types
     * @param proj_list     Projection of each attribute in the relation
     * @param n_out_flds    no of fields in the relation
     * @param n_pages       N_PAGES
     * @throws Exception
     */
    public GroupBywithSort(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                           Iterator am1, FldSpec group_by_attr, FldSpec[] agg_list,
                           AggType agg_type, FldSpec[] proj_list, int n_out_flds, int n_pages) throws Exception {
        this.attrType = in1;
        this.noOfColumns = (short) len_in1;
        this.strSize = t1_str_sizes;
        this.itr = am1;
        this.groupByAttr = group_by_attr;
        this.aggList = agg_list;
        this.aggType = agg_type;
        this.projList = proj_list;
        this.nOutFields = n_out_flds;
        this.nPages = n_pages;
        sortRelation();
    }

    /**
     * Perform sort on the given relation and stores sort result in a new heap file.
     * @throws Exception
     */
    private void sortRelation() throws Exception {
        TupleOrder order = new TupleOrder(TupleOrder.Ascending);
        Sort sortedRelation = new Sort(attrType, noOfColumns, strSize, itr, groupByAttr.offset, order, noOfColumns, nPages);
        sortedTuples = new Heapfile(SortedRelationName);
        Tuple tuple = null;
        while ((tuple = sortedRelation.get_next()) != null) {
            sortedTuples.insertRecord(new Tuple(tuple).returnTupleByteArray());
        }
    }

    /**
     *
     * @return a list of tuples for the desired aggregation operator.
     * @throws Exception
     */
    public List<Tuple> getAggregateResult() throws IOException, InvalidTypeException, WrongPermat, JoinsException, PredEvalException, UnknowAttrType, PageNotReadException, InvalidTupleSizeException, FieldNumberOutOfBoundException, FileScanException, TupleUtilsException, InvalidRelation {
        scan = new FileScan(SortedRelationName, attrType, strSize,
                noOfColumns, noOfColumns, projList, null);
        try {
            switch (aggType.aggType) {
                case AggType.aggMax:
                    return getMax();
                case AggType.aggMin:
                    return getMin();
                case AggType.aggAvg:
                    return getAvg();
                case AggType.aggSkyline:
                    return getSkyline();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        scan.close();
        return null;
    }

    /**
     *
     * @return list of tuples from each group which is min of that group based on agg_list.
     * @throws Exception
     */
    private List<Tuple> getMin() throws IOException, InvalidTypeException, PageNotReadException, JoinsException, PredEvalException, WrongPermat, UnknowAttrType, InvalidTupleSizeException, FieldNumberOutOfBoundException, TupleUtilsException {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MIN_VALUE;
        List<Tuple> minElementsOfEachGroup = new ArrayList<>();
        Tuple minTuple = null;
        float min = Float.MAX_VALUE;
        while ((tuple = scan.get_next()) != null) {
            tuple = new Tuple(tuple);
            int index = groupByAttr.offset;
            float groupByAttrValue = getAttrVal(tuple, index, attrType[index - 1]);
            if (previousGroupByAttrValue == Float.MIN_VALUE) {
                min = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                minTuple = tuple;
            } else if (previousGroupByAttrValue == groupByAttrValue) {
                float val = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                min = Math.min(val, min);
                if (min == val) {
                    minTuple = tuple;
                }
            } else {
                minElementsOfEachGroup.add(minTuple);
                min = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                minTuple = tuple;
            }
            previousGroupByAttrValue = groupByAttrValue;
        }
        minElementsOfEachGroup.add(minTuple);
        return minElementsOfEachGroup;
    }

    /**
     *
     * @return list of tuples from each group which is max of that group based on agg_list.
     * @throws Exception
     */
    private List<Tuple> getMax() throws IOException, InvalidTypeException, PageNotReadException, JoinsException, PredEvalException, WrongPermat, UnknowAttrType, InvalidTupleSizeException, FieldNumberOutOfBoundException, TupleUtilsException {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        List<Tuple> maxElementsOfEachGroup = new ArrayList<>();
        Tuple minTuple = null;
        float max = Float.MIN_VALUE;
        while ((tuple = scan.get_next()) != null) {
            tuple = new Tuple(tuple);
            int index = groupByAttr.offset;
            float groupByAttrValue = getAttrVal(tuple, index, attrType[index - 1]);
            if (previousGroupByAttrValue == Float.MAX_VALUE) {
                max = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                minTuple = tuple;
            } else if (previousGroupByAttrValue == groupByAttrValue) {
                float val = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                max = Math.max(val, max);
                if (max == val) {
                    minTuple = tuple;
                }
            } else {
                maxElementsOfEachGroup.add(minTuple);
                max = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                minTuple = tuple;
            }
            previousGroupByAttrValue = groupByAttrValue;
        }
        maxElementsOfEachGroup.add(minTuple);
        return maxElementsOfEachGroup;
    }

    // TO-DO: Value of non groupbyattr and non agg_list attr handling currently it is 0.

    /**
     *
     * @return list of tuples from each group having average of that group based on agg_list.
     * @throws Exception
     */
    private List<Tuple> getAvg() throws IOException, InvalidTypeException, PageNotReadException, JoinsException, PredEvalException, WrongPermat, UnknowAttrType, InvalidTupleSizeException, FieldNumberOutOfBoundException, TupleUtilsException {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        List<Tuple> avgElementsOfEachGroup = new ArrayList<>();
        float sum = 0;
        int count = 0;
        while ((tuple = scan.get_next()) != null) {
            tuple = new Tuple(tuple);
            int index = groupByAttr.offset;
            float groupByAttrValue = getAttrVal(tuple, index, attrType[index - 1]);
            if (previousGroupByAttrValue == Float.MAX_VALUE) {
                sum = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
            } else if (previousGroupByAttrValue == groupByAttrValue) {
                float val = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                sum += val;
            } else {
                Tuple t = getAvgResultTuple(previousGroupByAttrValue, sum, count);
                avgElementsOfEachGroup.add(t);
                count = 0;
                sum = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
            }
            count++;
            previousGroupByAttrValue = groupByAttrValue;
        }
        Tuple t = getAvgResultTuple(previousGroupByAttrValue, sum, count);
        avgElementsOfEachGroup.add(t);
        return avgElementsOfEachGroup;
    }

    /**
     *
     * @param groupByAttrValue  Value of Group by Attr for each group.
     * @param sum               sum of the attribute for each tuple in the group based on agg_list.
     * @param count             number of tuples in the group.
     * @return                  a tuple containing avg result for the getAvg() based on agg_list.
     * @throws Exception
     */
    private Tuple getAvgResultTuple(float groupByAttrValue, float sum, int count) throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException, TupleUtilsException {
        Tuple t = new Tuple();
        t.setHdr(noOfColumns, attrType, strSize);
        int size = t.size();
        t = new Tuple(size);
        t.setHdr(noOfColumns, attrType, strSize);
        for (int i = 1; i <= noOfColumns; i++) {
            if (attrType[i - 1].attrType == AttrType.attrInteger) {
                if (i == groupByAttr.offset) {
                    t.setIntFld(i, (int) groupByAttrValue);
                } else if (i == aggList[0].offset) {
                    t.setIntFld(i, (int) sum / count);
                } else {
                    t.setIntFld(i, 0);
                }
            } else if (attrType[i - 1].attrType == AttrType.attrReal) {
                if (i == groupByAttr.offset) {
                    t.setFloFld(i, groupByAttrValue);
                } else if (i == aggList[0].offset) {
                    t.setFloFld(i, sum / count);
                } else {
                    t.setFloFld(i, 0.0f);
                }
            } else {
                throw new TupleUtilsException("String Unsupported operator str");
            }
        }
        return t;
    }

    /**
     *
     * @return list of tuples from each group which are skyline of that group based on agg_list as pref_list.
     * @throws Exception
     */
    private List<Tuple> getSkyline() throws Exception {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        List<Tuple> skylineElementsOfEachGroup = new ArrayList<>();
        Heapfile file = new Heapfile("SkylineComputation.in");
        int[] prefList = new int[aggList.length];
        for (int i = 0; i < aggList.length; i++) {
            prefList[i] = aggList[i].offset;
        }
        while ((tuple = scan.get_next()) != null) {
            tuple = new Tuple(tuple);
            int index = groupByAttr.offset;
            float groupByAttrValue = getAttrVal(tuple, index, attrType[index - 1]);
            if (previousGroupByAttrValue == Float.MAX_VALUE) {
                file.insertRecord(tuple.returnTupleByteArray());
            } else if (previousGroupByAttrValue == groupByAttrValue) {
                file.insertRecord(tuple.returnTupleByteArray());
            } else {
                FileScan scan = new FileScan("SkylineComputation.in", attrType, strSize, noOfColumns, noOfColumns, projList, null);
                BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, noOfColumns, strSize, scan, "SkylineComputation.in", prefList, prefList.length, nPages);
                skylineElementsOfEachGroup.addAll(blockNestedLoopsSky.getAllSkylineMembers());
                scan.close();
                file.deleteFile();
                file = new Heapfile("SkylineComputation.in");
                file.insertRecord(tuple.returnTupleByteArray());
            }
            previousGroupByAttrValue = groupByAttrValue;
        }
        FileScan scan = new FileScan("SkylineComputation.in", attrType, strSize, noOfColumns, noOfColumns, projList, null);
        BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, noOfColumns, strSize, scan, "SkylineComputation.in", prefList, prefList.length, nPages);
        skylineElementsOfEachGroup.addAll(blockNestedLoopsSky.getAllSkylineMembers());
        scan.close();
        file.deleteFile();
        /**
         * last element
         */
        return skylineElementsOfEachGroup;
    }

    /**
     *
     * @param tuple         Tuple
     * @param index         attribute index whose value is desired
     * @param attrType      attribute type of the attribute whose value is required.
     * @return              get attribute value for a tuple
     * @throws Exception
     */
    private Float getAttrVal(Tuple tuple, int index, AttrType attrType) throws IOException, FieldNumberOutOfBoundException, TupleUtilsException {
        if (attrType.attrType == AttrType.attrInteger) {
            return (float) tuple.getIntFld(index);
        } else if (attrType.attrType == AttrType.attrReal) {
            return tuple.getFloFld(index);
        } else {
            throw new TupleUtilsException("String operation not supported");
        }
    }

    /**
     * Close the open instance of the intermediate heap file containing sorted tuples for the given group by attribute.
     * @throws Exception
     */
    public void close() throws HFDiskMgrException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException {
        sortedTuples.deleteFile();
    }

}
