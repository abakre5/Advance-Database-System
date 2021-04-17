package iterator;

import bufmgr.PageNotReadException;
import catalog.*;
import global.AggType;
import global.AttrType;
import global.ExtendedSystemDefs;
import global.TupleOrder;
import heap.*;
import tests.Phase3Driver;
import tests.Phase3Utils;

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
    private final String materTableName;

    private static final String SortedRelationName = "SortedTuples1";
    private final Heapfile materHeapfile;

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
     * @param materTableName
     * @throws Exception
     */
    public GroupBywithSort(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                           Iterator am1, FldSpec group_by_attr, FldSpec[] agg_list,
                           AggType agg_type, FldSpec[] proj_list, int n_out_flds, int n_pages, String materTableName) throws Exception {
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
        this.materTableName = materTableName;

        this.materHeapfile = new Heapfile(materTableName);

        if (Phase3Utils.aggListContainsStringAttr(agg_list, in1)) {
            System.err.println("Aggregation attributes does not support String attribute!");
            return;
        }

        //if (!isRelationSortedOnGroupByAttr()) {
            System.out.println("Sorting Relation based on group by Attr.");
            Tuple t = null;
            sortRelation();
            System.out.println("Sorting finished!");
        //} else {
//            System.out.println("Relation is already sorted on Group By Attr.");
//            scan = (FileScan) itr;
        //}
    }
    /**
     *
     * @return
     * @throws Exception
     */
    private boolean isRelationSortedOnGroupByAttr() throws Exception {
        Tuple curr = itr.get_next();
        while (curr != null) {
            curr = new Tuple(curr);
            Tuple next = itr.get_next();
            if (next != null) {
                next = new Tuple(next);
                int isT1GreaterThanT2 = TupleUtils.CompareTupleWithTuple(attrType[groupByAttr.offset], curr, groupByAttr.offset, next, groupByAttr.offset);
                if (isT1GreaterThanT2 == 1) {
                    return false;
                }
            }
            curr = next;
        }
        return true;
    }

    /**
     * Perform sort on the given relation and stores sort result in a new heap file.
     *
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
     * @return a list of tuples for the desired aggregation operator.
     * @throws Exception
     */
    public void getAggregateResult() throws IOException, InvalidTypeException, WrongPermat, JoinsException, PredEvalException, UnknowAttrType, PageNotReadException, InvalidTupleSizeException, FieldNumberOutOfBoundException, FileScanException, TupleUtilsException, InvalidRelation, FileAlreadyDeletedException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        scan = new FileScan(SortedRelationName, attrType, strSize,
                noOfColumns, noOfColumns, projList, null);
        System.out.println("Computing " + aggType.toString() + " ...");
        try {
            switch (aggType.aggType) {
                case AggType.aggMax:
                    getMax();
                    break;
                case AggType.aggMin:
                    getMin();
                    break;
                case AggType.aggAvg:
                    getAvg();
                    break;
                case AggType.aggSkyline:
                    getSkyline();
                    break;
            }
        } catch (Exception e) {
            System.out.println("Error occurred -> " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Done!");
        scan.close();
        sortedTuples.deleteFile();
        //Phase3Utils.writeToDisk();
    }

    /**
     * @return list of tuples from each group which is min of that group based on agg_list.
     * @throws Exception
     */
    private List<Tuple> getMin() throws IOException, InvalidTypeException, PageNotReadException, JoinsException, PredEvalException, WrongPermat, UnknowAttrType, InvalidTupleSizeException, FieldNumberOutOfBoundException, TupleUtilsException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException, Catalogrelexists, Catalogmissparam, Catalognomem, RelCatalogException, Cataloghferror, Catalogdupattrs, Catalogioerror {
        int SIZE_OF_INT = 4;
        attrInfo[] attrs = new attrInfo[2];

        attrs[0] = new attrInfo();
        attrs[0].attrType = new AttrType(attrType[groupByAttr.offset - 1].attrType);
        attrs[0].attrName = "Col" + 0;
        attrs[0].attrLen = (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 0;

        attrs[1] = new attrInfo();
        attrs[1].attrType = new AttrType(attrType[aggList[0].offset - 1].attrType);
        attrs[1].attrName = "Col" + 1;
        attrs[1].attrLen = (attrType[aggList[0].offset - 1].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 0;

        ExtendedSystemDefs.MINIBASE_RELCAT.createRel(materTableName, 2, attrs);
        Tuple tuple;
        float previousGroupByAttrValue = Float.MIN_VALUE;
        List<Tuple> minElementsOfEachGroup = new ArrayList<>();
        Tuple minTuple = null;
        float min = Float.MAX_VALUE;
        float groupByAttrValue = 0;
        int rows = 0;
        while ((tuple = scan.get_next()) != null) {
            tuple = new Tuple(tuple);
            int index = groupByAttr.offset;
            groupByAttrValue = getAttrVal(tuple, index, attrType[index - 1]);
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
                materHeapfile.insertRecord(new Tuple(getAggTuple(groupByAttrValue, groupByAttr.offset, min,
                        attrType[aggList[0].offset - 1])).returnTupleByteArray());
                minElementsOfEachGroup.add(new Tuple(minTuple));
                min = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                minTuple = tuple;
                rows++;
            }
            previousGroupByAttrValue = groupByAttrValue;
        }
        assert minTuple != null;
        rows++;
        materHeapfile.insertRecord(new Tuple(getAggTuple(groupByAttrValue, groupByAttr.offset, min,
                attrType[aggList[0].offset - 1])).returnTupleByteArray());
        minElementsOfEachGroup.add(new Tuple(minTuple));
        System.out.println("No of rows in group by " + rows);
        return minElementsOfEachGroup;
    }

    private Tuple getAggTuple(float groupByAttrValue, int groupByAttr, float aggVal, AttrType aggAttrType) {
        Tuple t = new Tuple();
        try {
            AttrType[] attrTypes = {attrType[groupByAttr - 1], aggAttrType};
            short[] strSizes = new short[1];
            try {
                t.setHdr((short) 2, attrTypes, strSizes);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (attrType[groupByAttr - 1].attrType == AttrType.attrInteger) {
                t.setIntFld(1, (int) groupByAttrValue);
            } else {
                t.setFloFld(1, groupByAttrValue);
            }
            if (aggAttrType.attrType == AttrType.attrInteger) {
                t.setIntFld(2, (int) aggVal);
            } else {
                t.setFloFld(2, aggVal);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return t;
    }

    /**
     * @return list of tuples from each group which is max of that group based on agg_list.
     * @throws Exception
     */
    private List<Tuple> getMax() throws IOException, InvalidTypeException, PageNotReadException, JoinsException, PredEvalException, WrongPermat, UnknowAttrType, InvalidTupleSizeException, FieldNumberOutOfBoundException, TupleUtilsException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException, Catalogrelexists, Catalogmissparam, Catalognomem, RelCatalogException, Cataloghferror, Catalogdupattrs, Catalogioerror {

        int SIZE_OF_INT = 4;
        attrInfo[] attrs = new attrInfo[2];

        attrs[0] = new attrInfo();
        attrs[0].attrType = new AttrType(attrType[groupByAttr.offset - 1].attrType);
        attrs[0].attrName = "Col" + 0;
        attrs[0].attrLen = (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 0;

        attrs[1] = new attrInfo();
        attrs[1].attrType = new AttrType(attrType[aggList[0].offset - 1].attrType);
        attrs[1].attrName = "Col" + 1;
        attrs[1].attrLen = (attrType[aggList[0].offset - 1].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 0;
        ExtendedSystemDefs.MINIBASE_RELCAT.createRel(materTableName, 2, attrs);

        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        List<Tuple> maxElementsOfEachGroup = new ArrayList<>();
        Tuple minTuple = null;
        float max = Float.MIN_VALUE;
        float groupByAttrValue = 0;
        while ((tuple = scan.get_next()) != null) {
            tuple = new Tuple(tuple);
            int index = groupByAttr.offset;
            groupByAttrValue = getAttrVal(tuple, index, attrType[index - 1]);
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
                materHeapfile.insertRecord(new Tuple(getAggTuple(groupByAttrValue, groupByAttr.offset, max,
                        attrType[aggList[0].offset - 1])).returnTupleByteArray());
                max = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                minTuple = tuple;
            }
            previousGroupByAttrValue = groupByAttrValue;
        }
        maxElementsOfEachGroup.add(minTuple);
        materHeapfile.insertRecord(new Tuple(getAggTuple(groupByAttrValue, groupByAttr.offset, max,
                attrType[aggList[0].offset - 1])).returnTupleByteArray());
        assert minTuple != null;
        return maxElementsOfEachGroup;
    }

    // TO-DO: Value of non groupbyattr and non agg_list attr handling currently it is 0.

    /**
     * @return list of tuples from each group having average of that group based on agg_list.
     * @throws Exception
     */
    private List<Tuple> getAvg() throws IOException, InvalidTypeException, PageNotReadException, JoinsException, PredEvalException, WrongPermat, UnknowAttrType, InvalidTupleSizeException, FieldNumberOutOfBoundException, TupleUtilsException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException, Catalogrelexists, Catalogmissparam, Catalognomem, RelCatalogException, Cataloghferror, Catalogdupattrs, Catalogioerror {
        int SIZE_OF_INT = 4;
        attrInfo[] attrs = new attrInfo[2];

        attrs[0] = new attrInfo();
        attrs[0].attrType = new AttrType(attrType[groupByAttr.offset - 1].attrType);
        attrs[0].attrName = "Col" + 0;
        attrs[0].attrLen = (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 0;

        attrs[1] = new attrInfo();
        attrs[1].attrType = new AttrType(AttrType.attrReal);
        attrs[1].attrName = "Col" + 1;
        attrs[1].attrLen = 4;

        ExtendedSystemDefs.MINIBASE_RELCAT.createRel(materTableName, 2, attrs);
        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        List<Tuple> avgElementsOfEachGroup = new ArrayList<>();
        float sum = 0;
        int count = 0;
        float groupByAttrValue = 0;
        while ((tuple = scan.get_next()) != null) {
            tuple = new Tuple(tuple);
            int index = groupByAttr.offset;
            groupByAttrValue = getAttrVal(tuple, index, attrType[index - 1]);
            if (previousGroupByAttrValue == Float.MAX_VALUE) {
                sum = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
            } else if (previousGroupByAttrValue == groupByAttrValue) {
                float val = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                sum += val;
            } else {
                Tuple t = getAggTuple(previousGroupByAttrValue, groupByAttr.offset, sum / count, new AttrType(AttrType.attrReal));
                materHeapfile.insertRecord(new Tuple(t).returnTupleByteArray());
                avgElementsOfEachGroup.add(t);
                count = 0;
                sum = getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
            }
            count++;
            previousGroupByAttrValue = groupByAttrValue;
        }
        Tuple t = getAggTuple(previousGroupByAttrValue, groupByAttr.offset, sum / count, new AttrType(AttrType.attrReal));
        materHeapfile.insertRecord(new Tuple(t).returnTupleByteArray());
        avgElementsOfEachGroup.add(t);
        return avgElementsOfEachGroup;
    }

    /**
     *
     *
     *
     *
     *
     *
     *
     * CHECK FOR THIS
     *
     *
     *
     *
     *
     *
     */
    /**
     * @return list of tuples from each group which are skyline of that group based on agg_list as pref_list.
     * @throws Exception
     */
    private List<Tuple> getSkyline() throws Exception {
        int SIZE_OF_INT = 4;
        attrInfo[] attrs = new attrInfo[noOfColumns];

        for (int i = 0; i < noOfColumns; ++i) {
            attrs[i] = new attrInfo();
            attrs[i].attrType = new AttrType(attrType[i].attrType);
            attrs[i].attrName = "Col" + i;
            attrs[i].attrLen = (attrType[i].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 32;
        }

        ExtendedSystemDefs.MINIBASE_RELCAT.createRel(materTableName, noOfColumns, attrs);

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
                //skylineElementsOfEachGroup.addAll(blockNestedLoopsSky.getAllSkylineMembers());
                Phase3Utils.insertIntoTable(blockNestedLoopsSky.getAllSkylineMembers(), materHeapfile);
                scan.close();
                file.deleteFile();
                file = new Heapfile("SkylineComputation.in");
                file.insertRecord(tuple.returnTupleByteArray());
            }
            previousGroupByAttrValue = groupByAttrValue;
        }

        FileScan scan = new FileScan("SkylineComputation.in", attrType, strSize, noOfColumns, noOfColumns, projList, null);
        BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, noOfColumns, strSize, scan, "SkylineComputation.in", prefList, prefList.length, nPages);
        //skylineElementsOfEachGroup.addAll(blockNestedLoopsSky.getAllSkylineMembers());
        Phase3Utils.insertIntoTable(blockNestedLoopsSky.getAllSkylineMembers(), materHeapfile);
        scan.close();
        file.deleteFile();
        /**
         * last element
         */
        return skylineElementsOfEachGroup;
    }

    /**
     * @param tuple    Tuple
     * @param index    attribute index whose value is desired
     * @param attrType attribute type of the attribute whose value is required.
     * @return get attribute value for a tuple
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
     *
     * @throws Exception
     */
    public void close() throws HFDiskMgrException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException {
        sortedTuples.deleteFile();
    }

}
