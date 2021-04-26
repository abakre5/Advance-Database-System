package iterator;

import bufmgr.PageNotReadException;
import catalog.*;
import global.AggType;
import global.AttrType;
import global.IndexType;
import global.RID;
import hash.HashFile;
import hash.HashIndexFileScan;
import hash.HashUnclusteredFileScan;
import heap.*;
import index.IndexUtils;
import tests.Phase3Utils;

import java.io.IOException;

/**
 * @author Abhishek Bakare
 */
public class GroupBywithHash {
    private final AttrType[] attrType;
    private final short noOfColumns;
    private final short[] strSize;
    private final String tableName;
    private final FldSpec groupByAttr;
    private final FldSpec[] aggList;
    private final AggType aggType;
    private final FldSpec[] projList;
    private final int nOutFields;
    private final int nPages;
    private Heapfile materHeapfile;
    private Heapfile dbHeapFile;

    private HashFile hashFile;
    private final String materTableName;

    private String hashIndexName;

    private Heapfile hashBucketTuples;
    private HashIndexFileScan hashScan;
    private int countX;

    /**
     * Perform Group by operation on a relation using @Sort class of the Minibase.
     *
     * @param in1           Attribute Types in the relation
     * @param len_in1       No Of Columns in the relation
     * @param t1_str_sizes  String size in the relation if any
     * @param tableName
     * @param group_by_attr Attribute on which group by needs to be performed
     * @param agg_list      Aggregation Attributes
     * @param agg_type      Aggregation Attributes types
     * @param proj_list     Projection of each attribute in the relation
     * @param n_out_flds    no of fields in the relation
     * @param n_pages       N_PAGES
     * @param materTableName
     * @throws Exception
     */
    public GroupBywithHash(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                           String tableName, FldSpec group_by_attr, FldSpec[] agg_list,
                           AggType agg_type, FldSpec[] proj_list, int n_out_flds, int n_pages, String materTableName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, FileScanException, TupleUtilsException, InvalidRelation, JoinsException, FieldNumberOutOfBoundException, PageNotReadException, WrongPermat, InvalidTypeException, InvalidTupleSizeException, PredEvalException, UnknowAttrType, InvalidSlotNumberException {
        this.attrType = in1;
        this.noOfColumns = (short) len_in1;
        this.strSize = t1_str_sizes;
        this.tableName = tableName;
        this.groupByAttr = group_by_attr;
        this.aggList = agg_list;
        this.aggType = agg_type;
        this.projList = proj_list;
        this.nOutFields = n_out_flds;
        this.nPages = n_pages;
        this.materTableName = materTableName;
        this.materHeapfile = null;
        this.countX = 0;

        if (Phase3Utils.aggListContainsStringAttr(agg_list, in1)) {
            System.err.println("Aggregation attributes does not support String attribute!");
            return;
        }

        if (Phase3Utils.createMaterializedView(materTableName)) {
            this.materHeapfile = new Heapfile(materTableName);
        }

        this.hashIndexName = tableName + "BHs" + (groupByAttr.offset);
        System.out.println("Hash Index name: " + this.hashIndexName);

        this.dbHeapFile = new Heapfile(tableName);
        System.out.println("No of elements: " + noOfColumns);
        System.out.println("Attr type no: " + in1.length);
        System.out.println("Project No : " + proj_list.length);
        if (!ifIndexExistOnTheTable()) {
            try {
                System.out.println("Creating Unclustered Hash Index on table " + tableName + " and attribute " + group_by_attr.offset);
                hashFile = new HashFile(tableName, hashIndexName, group_by_attr.offset,
                        attrType[groupByAttr.offset - 1].attrType,
                        null, dbHeapFile.getRecCnt(), dbHeapFile, attrType, strSize, noOfColumns);
            } catch (Exception e) {
                System.err.println("Error occurred while creating Index.");
                return;
            }
            Phase3Utils.insertIndexEntry(tableName, group_by_attr.offset, IndexType.Hash);
            System.out.println("Hash Index created on group by attr -> " + groupByAttr.offset);
        } else {
            hashFile = new HashFile(tableName, hashIndexName, group_by_attr.offset, attrType[groupByAttr.offset - 1].attrType, 0, dbHeapFile,attrType, strSize, noOfColumns);
            System.out.println("Reusing hash index created on group by attr -> " + groupByAttr.offset);
        }
    }

    private boolean ifIndexExistOnTheTable() {
        return Phase3Utils.isIndexExists(tableName, groupByAttr.offset, IndexType.Hash);
    }

    /**
     * @return a list of tuples for the desired aggregation operator.
     * @throws Exception
     */
    public void getAggregateResult() throws IOException, InvalidTypeException, WrongPermat, JoinsException, PredEvalException, UnknowAttrType, PageNotReadException, InvalidTupleSizeException, FieldNumberOutOfBoundException, FileScanException, TupleUtilsException, InvalidRelation {
        try {
            hashScan = (HashUnclusteredFileScan) IndexUtils.HashUnclusteredScan(hashFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
            countX = 0;
        } catch (Exception e) {
            System.out.println("Error occurred -> " + e.getMessage());
            e.printStackTrace();
            try {
                materHeapfile.deleteFile();
            } catch (InvalidSlotNumberException | FileAlreadyDeletedException | InvalidTupleSizeException | HFBufMgrException | HFDiskMgrException | IOException ee) {
                ee.printStackTrace();
            }
        } finally {
            System.out.println("Done computing group by operation!");

        }
    }

    /**
     * @return list of tuples from each group which is min of that group based on agg_list.
     * @throws Exception
     */
    private void getMin() throws Exception {

        attrInfo[] attrs = Phase3Utils.getAttrInfoGroupBy(attrType, groupByAttr, aggList);
        Phase3Utils.createTable(materTableName, 2, attrs);

        AttrType[] groupByAttrTypes = Phase3Utils.getGroupByAttrTypes(attrs);

        if (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) {
            computeMinGroupByAttrInt(groupByAttrTypes);
        } else {
            computeMinGroupByAttrString(groupByAttrTypes);
        }

    }


    /**
     *
     * Generate list of tuples from each group which is min(int) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeMinGroupByAttrInt(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MIN_VALUE;
        Tuple minTuple = null;
        float min = Float.MAX_VALUE;
        float groupByAttrValue = 0;
        int rows = 0;
        try {
            hash.KeyDataEntry entry = hashScan.get_next();
            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                groupByAttrValue = Phase3Utils.getAttrVal(tuple, index, attrType[index - 1]);
                if (previousGroupByAttrValue == Float.MIN_VALUE) {
                    min = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    minTuple = tuple;
                } else if (previousGroupByAttrValue == groupByAttrValue) {
                    float val = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    min = Math.min(val, min);
                    if (min == val) {
                        minTuple = tuple;
                    }
                } else {
                    Tuple to = new Tuple(Phase3Utils.getAggTuple(previousGroupByAttrValue, groupByAttr.offset, min,
                            attrType[aggList[0].offset - 1], attrType));
                    min = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    minTuple = tuple;
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        materHeapfile.insertRecord(to.returnTupleByteArray());
                    } else {
                        to.print(groupByAttrTypes);
                    }
                    rows++;
                }
                previousGroupByAttrValue = groupByAttrValue;

                entry = hashScan.get_next();
            }
            assert minTuple != null;
            rows++;
            Tuple to = new Tuple(Phase3Utils.getAggTuple(groupByAttrValue, groupByAttr.offset, min,
                    attrType[aggList[0].offset - 1], attrType));
            if (Phase3Utils.createMaterializedView(materTableName)) {
                materHeapfile.insertRecord(to.returnTupleByteArray());
            } else {
                to.print(groupByAttrTypes);
            }
        } catch (Exception e) {
            System.out.println("Error occurred while computing group by MIN!" + e);
            e.printStackTrace();
        }
        System.out.println("No of rows in group by " + rows);
    }


    /**
     * Generate list of tuples from each group which is min(String) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeMinGroupByAttrString(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        String previousGroupByAttrValue = Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER;
        float min = Float.MAX_VALUE;
        String groupByAttrValue = "";
        int rows = 0;
        try {
            hash.KeyDataEntry entry = hashScan.get_next();
            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                groupByAttrValue = Phase3Utils.getAttrValString(tuple, index, attrType[index - 1]);
                if (previousGroupByAttrValue.equals(Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER)) {
                    min = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                } else if (previousGroupByAttrValue.equals(groupByAttrValue)) {
                    float val = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    min = Math.min(val, min);
                } else {
                    Tuple to = new Tuple(Phase3Utils.getAggTupleGroupByAttrString(previousGroupByAttrValue, groupByAttr.offset, min,
                            attrType[aggList[0].offset - 1]));
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        materHeapfile.insertRecord(to.returnTupleByteArray());
                    } else {
                        to.print(groupByAttrTypes);
                    }
                    min = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    rows++;
                }
                previousGroupByAttrValue = groupByAttrValue;
                entry = hashScan.get_next();
            }
            rows++;
            Tuple to = new Tuple(Phase3Utils.getAggTupleGroupByAttrString(groupByAttrValue, groupByAttr.offset, min,
                    attrType[aggList[0].offset - 1]));
            if (Phase3Utils.createMaterializedView(materTableName)) {
                materHeapfile.insertRecord(to.returnTupleByteArray());
            } else {
                to.print(groupByAttrTypes);
            }
        } catch (Exception e) {
            System.out.println("Error occurred while computing group by MAX!" + e);
            e.printStackTrace();
        }
        System.out.println("No of rows in group by " + rows);
    }

    /**
     * @return list of tuples from each group which is max of that group based on agg_list.
     * @throws Exception
     */
    private void getMax() throws Exception {

        attrInfo[] attrs = Phase3Utils.getAttrInfoGroupBy(attrType, groupByAttr, aggList);
        Phase3Utils.createTable(materTableName, 2, attrs);

        AttrType[] groupByAttrTypes = Phase3Utils.getGroupByAttrTypes(attrs);

        if (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) {
            computeMaxGroupByAttrInt(groupByAttrTypes);
        } else {
            computeMaxGroupByAttrString(groupByAttrTypes);
        }

    }


    /**
     * Generate list of tuples from each group which is max(int) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeMaxGroupByAttrInt(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;

        float groupByAttrValue = 0;
        int rows = 0;
        try {
            hash.KeyDataEntry entry = hashScan.get_next();
            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                groupByAttrValue = Phase3Utils.getAttrVal(tuple, index, attrType[index - 1]);
                if (previousGroupByAttrValue == Float.MAX_VALUE) {
                    max = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                } else if (previousGroupByAttrValue == groupByAttrValue) {
                    float val = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    max = Math.max(val, max);
                } else {
                    Tuple to = new Tuple(Phase3Utils.getAggTuple(previousGroupByAttrValue, groupByAttr.offset, max,
                            attrType[aggList[0].offset - 1], attrType));
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        materHeapfile.insertRecord(to.returnTupleByteArray());
                    } else {
                        to.print(groupByAttrTypes);
                    }
                    max = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    rows++;
                }
                previousGroupByAttrValue = groupByAttrValue;
                entry = hashScan.get_next();
            }
            rows++;
            Tuple to = new Tuple(Phase3Utils.getAggTuple(groupByAttrValue, groupByAttr.offset, max,
                    attrType[aggList[0].offset - 1], attrType));
            if (Phase3Utils.createMaterializedView(materTableName)) {
                materHeapfile.insertRecord(to.returnTupleByteArray());
            } else {
                to.print(groupByAttrTypes);
            }
        } catch (Exception e) {
            System.out.println("Error occurred while computing group by MAX!" + e);
            e.printStackTrace();
        }
        System.out.println("No of rows in group by " + rows);
    }

    /**
     * Generate list of tuples from each group which is Max(String) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeMaxGroupByAttrString(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        String previousGroupByAttrValue = Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER;
        float max = Float.MIN_VALUE;

        String groupByAttrValue = "";
        int rows = 0;
        try {
            hash.KeyDataEntry entry = hashScan.get_next();
            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                groupByAttrValue = Phase3Utils.getAttrValString(tuple, index, attrType[index - 1]);
                if (previousGroupByAttrValue.equals(Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER)) {
                    max = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                } else if (previousGroupByAttrValue.equals(groupByAttrValue)) {
                    float val = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                    max = Math.max(val, max);
                } else {
                    Tuple to = new Tuple(Phase3Utils.getAggTupleGroupByAttrString(previousGroupByAttrValue, groupByAttr.offset, max,
                            attrType[aggList[0].offset - 1]));
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        materHeapfile.insertRecord(to.returnTupleByteArray());
                    } else {
                        to.print(groupByAttrTypes);
                    }
                    rows++;
                    max = Phase3Utils.getAttrVal(tuple, aggList[0].offset, attrType[aggList[0].offset - 1]);
                }
                previousGroupByAttrValue = groupByAttrValue;
                entry = hashScan.get_next();
            }

            Tuple to = new Tuple(Phase3Utils.getAggTupleGroupByAttrString(groupByAttrValue, groupByAttr.offset, max,
                    attrType[aggList[0].offset - 1]));
            if (Phase3Utils.createMaterializedView(materTableName)) {
                materHeapfile.insertRecord(to.returnTupleByteArray());
            } else {
                to.print(groupByAttrTypes);
            }
            rows++;
        } catch (Exception e) {
            System.out.println("Error occurred while computing group by MAX!" + e);
            e.printStackTrace();
        }

        System.out.println("No of rows in group by " + rows);
    }

    /**
     * @return list of tuples from each group having average of that group based on agg_list.
     * @throws Exception
     */
    private void getAvg() throws Exception {

        attrInfo[] attrs = Phase3Utils.getAttrInfoGroupBy(attrType, groupByAttr, aggList);
        attrs[1].attrType = new AttrType(AttrType.attrReal);
        Phase3Utils.createTable(materTableName, 2, attrs);

        if (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) {
            computeAvgGroupByAttrInt(attrType);
        } else {
            computeAvgGroupByAttrString(attrType);
        }


    }


    /**
     * Generate list of tuples from each group which is avg(int) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeAvgGroupByAttrInt(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        float sum = 0;
        int count = 0;
        float groupByAttrValue = 0;
        int rows = 0;
        try {
            hash.KeyDataEntry entry = hashScan.get_next();
            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                groupByAttrValue = Phase3Utils.getAttrVal(tuple, index, this.attrType[index - 1]);
                if (previousGroupByAttrValue == Float.MAX_VALUE) {
                    sum = Phase3Utils.getAttrVal(tuple, aggList[0].offset, this.attrType[aggList[0].offset - 1]);
                } else if (previousGroupByAttrValue == groupByAttrValue) {
                    float val = Phase3Utils.getAttrVal(tuple, aggList[0].offset, this.attrType[aggList[0].offset - 1]);
                    sum += val;
                } else {
                    Tuple to = new Tuple(Phase3Utils.getAggTuple(previousGroupByAttrValue, groupByAttr.offset, sum / count, new AttrType(AttrType.attrReal), this.attrType));
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        materHeapfile.insertRecord(to.returnTupleByteArray());
                    } else {
                        to.print(groupByAttrTypes);
                    }
                    count = 0;
                    rows++;
                    sum = Phase3Utils.getAttrVal(tuple, aggList[0].offset, this.attrType[aggList[0].offset - 1]);
                }
                count++;
                previousGroupByAttrValue = groupByAttrValue;
                entry = hashScan.get_next();
            }

            Tuple to = new Tuple(Phase3Utils.getAggTuple(previousGroupByAttrValue, groupByAttr.offset, sum / count, new AttrType(AttrType.attrReal), this.attrType));
            if (Phase3Utils.createMaterializedView(materTableName)) {
                materHeapfile.insertRecord(to.returnTupleByteArray());
            } else {
                to.print(groupByAttrTypes);
            }
            rows++;
        } catch (Exception e) {
            System.out.println("Error occurred while computing group by avg!" + e);
            e.printStackTrace();
        }

        System.out.println("No of rows in group by " + rows);
    }

    /**
     * Generate list of tuples from each group which is avg(String) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeAvgGroupByAttrString(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        String previousGroupByAttrValue = Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER;
        float sum = 0;
        int count = 0;
        String groupByAttrValue = "";
        int rows = 0;
        try {
            hash.KeyDataEntry entry = hashScan.get_next();
            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                groupByAttrValue = Phase3Utils.getAttrValString(tuple, index, this.attrType[index - 1]);
                if (previousGroupByAttrValue.equals(Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER)) {
                    sum = Phase3Utils.getAttrVal(tuple, aggList[0].offset, this.attrType[aggList[0].offset - 1]);
                } else if (previousGroupByAttrValue.equals(groupByAttrValue)) {
                    float val = Phase3Utils.getAttrVal(tuple, aggList[0].offset, this.attrType[aggList[0].offset - 1]);
                    sum += val;
                } else {
                    Tuple to = new Tuple(Phase3Utils.getAggTupleGroupByAttrString(previousGroupByAttrValue, groupByAttr.offset, sum / count, new AttrType(AttrType.attrReal)));
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        materHeapfile.insertRecord(to.returnTupleByteArray());
                    } else {
                        to.print(groupByAttrTypes);
                    }
                    rows++;
                    count = 0;
                    sum = Phase3Utils.getAttrVal(tuple, aggList[0].offset, this.attrType[aggList[0].offset - 1]);
                }
                count++;
                previousGroupByAttrValue = groupByAttrValue;
                entry = hashScan.get_next();
            }
            Tuple to = new Tuple(Phase3Utils.getAggTupleGroupByAttrString(previousGroupByAttrValue, groupByAttr.offset, sum / count, new AttrType(AttrType.attrReal)));
            if (Phase3Utils.createMaterializedView(materTableName)) {
                materHeapfile.insertRecord(to.returnTupleByteArray());
            } else {
                to.print(groupByAttrTypes);
            }
            rows++;
        } catch (Exception e) {
            System.out.println("Error occurred while computing group by avg!" + e);
            e.printStackTrace();
        }
        System.out.println("No of rows in group by " + rows);
    }


    /**
     * @return list of tuples from each group which are skyline of that group based on agg_list as pref_list.
     * @throws Exception
     */
    private void getSkyline() throws Exception {
        attrInfo[] attrs = new attrInfo[noOfColumns];

        for (int i = 0; i < noOfColumns; ++i) {
            attrs[i] = new attrInfo();
            attrs[i].attrType = new AttrType(attrType[i].attrType);
            attrs[i].attrName = "Col" + i;
            attrs[i].attrLen = (attrType[i].attrType == AttrType.attrInteger) ? Phase3Utils.SIZE_OF_INT : Phase3Utils.SIZE_OF_STRING;
        }

        AttrType[] groupByAttrTypes = Phase3Utils.getGroupByAttrTypes(attrs);
        Phase3Utils.createTable(materTableName, noOfColumns, attrs);

        if (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) {
            computeSkyGroupByAttrInt(groupByAttrTypes);
        } else {
            computeSkyGroupByAttrString(groupByAttrTypes);
        }

    }

    /**
     * Generate list of tuples from each group which is sky(int) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeSkyGroupByAttrInt(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        float previousGroupByAttrValue = Float.MAX_VALUE;
        Heapfile file = null;
        FileScan scan = null;
        try {
            file = new Heapfile("SkylineComputation.in");
            hash.KeyDataEntry entry = hashScan.get_next();
            int[] prefList = new int[aggList.length];
            for (int i = 0; i < aggList.length; i++) {
                prefList[i] = aggList[i].offset;
            }

            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                float groupByAttrValue = Phase3Utils.getAttrVal(tuple, index, attrType[index - 1]);
                if (previousGroupByAttrValue == Float.MAX_VALUE) {
                    file.insertRecord(tuple.returnTupleByteArray());
                } else if (previousGroupByAttrValue == groupByAttrValue) {
                    file.insertRecord(tuple.returnTupleByteArray());
                } else {
                    scan = new FileScan("SkylineComputation.in", attrType, strSize, noOfColumns, noOfColumns, projList, null);
                    BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, noOfColumns, strSize, scan, "SkylineComputation.in", prefList, prefList.length, nPages);
                    countX += blockNestedLoopsSky.getAllSkylineMembers().size();
                    System.out.println("No of skyline for group " + (int)previousGroupByAttrValue  + " are " + blockNestedLoopsSky.getAllSkylineMembers().size());
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        Phase3Utils.insertIntoTable(blockNestedLoopsSky.getAllSkylineMembers(), materHeapfile);
                    } else {
                        Phase3Utils.printTuples(blockNestedLoopsSky.getAllSkylineMembers(), groupByAttrTypes);
                    }

                    scan.close();
                    file.deleteFile();
                    file = new Heapfile("SkylineComputation.in");
                    file.insertRecord(tuple.returnTupleByteArray());
                }
                previousGroupByAttrValue = groupByAttrValue;
                entry = hashScan.get_next();
            }

            scan = new FileScan("SkylineComputation.in", attrType, strSize, noOfColumns, noOfColumns, projList, null);
            BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, noOfColumns, strSize, scan, "SkylineComputation.in", prefList, prefList.length, nPages);
            countX += blockNestedLoopsSky.getAllSkylineMembers().size();
            System.out.println("No of skyline for group " + (int)previousGroupByAttrValue  + " are " + blockNestedLoopsSky.getAllSkylineMembers().size());
            if (Phase3Utils.createMaterializedView(materTableName)) {
                Phase3Utils.insertIntoTable(blockNestedLoopsSky.getAllSkylineMembers(), materHeapfile);
            } else {
                Phase3Utils.printTuples(blockNestedLoopsSky.getAllSkylineMembers(), groupByAttrTypes);
            }
        } catch (Exception e) {
            System.out.println("Error occurred while Group By Skyline Computation!");
            e.printStackTrace();
        } finally {
            Phase3Utils.closeScan(scan);
        }
    }

    /**
     * Generate list of tuples from each group which is sky(String) of that group based on agg_list.
     *
     * @param groupByAttrTypes attributes in final output relation
     * @throws Exception
     */
    private void computeSkyGroupByAttrString(AttrType[] groupByAttrTypes) throws Exception {
        Tuple tuple;
        String previousGroupByAttrValue = Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER;
        Heapfile file = null;
        FileScan scan = null;
        try {
            file = new Heapfile("SkylineComputation.in");
            hash.KeyDataEntry entry = hashScan.get_next();
            int[] prefList = new int[aggList.length];
            for (int i = 0; i < aggList.length; i++) {
                prefList[i] = aggList[i].offset;
            }
            while (entry != null) {
                RID fetchRID = entry.data;
                tuple = dbHeapFile.getRecord(fetchRID);
                tuple = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tuple.setHdr(noOfColumns, attrType, strSize);
                int index = groupByAttr.offset;
                String groupByAttrValue = Phase3Utils.getAttrValString(tuple, index, attrType[index - 1]);
                if (previousGroupByAttrValue.equals(Phase3Utils.GROUP_BY_ATTR_STRING_INITIALIZER)) {
                    file.insertRecord(tuple.returnTupleByteArray());
                } else if (previousGroupByAttrValue.equals(groupByAttrValue)) {
                    file.insertRecord(tuple.returnTupleByteArray());
                } else {
                    scan = new FileScan("SkylineComputation.in", attrType, strSize, noOfColumns, noOfColumns, projList, null);
                    BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, noOfColumns, strSize, scan, "SkylineComputation.in", prefList, prefList.length, nPages);
                    countX += blockNestedLoopsSky.getAllSkylineMembers().size();
                    System.out.println("No of skyline for group " + previousGroupByAttrValue  + " are " + blockNestedLoopsSky.getAllSkylineMembers().size());
                    if (Phase3Utils.createMaterializedView(materTableName)) {
                        Phase3Utils.insertIntoTable(blockNestedLoopsSky.getAllSkylineMembers(), materHeapfile);
                    } else {
                        Phase3Utils.printTuples(blockNestedLoopsSky.getAllSkylineMembers(), groupByAttrTypes);
                    }
                    Phase3Utils.closeScan(scan);
                    file.deleteFile();
                    file = new Heapfile("SkylineComputation.in");
                    file.insertRecord(tuple.returnTupleByteArray());
                }
                previousGroupByAttrValue = groupByAttrValue;
                entry = hashScan.get_next();
            }

            scan = new FileScan("SkylineComputation.in", attrType, strSize, noOfColumns, noOfColumns, projList, null);
            BlockNestedLoopsSky blockNestedLoopsSky = new BlockNestedLoopsSky(attrType, noOfColumns, strSize, scan, "SkylineComputation.in", prefList, prefList.length, nPages);
            countX += blockNestedLoopsSky.getAllSkylineMembers().size();
            System.out.println("No of skyline for group " + previousGroupByAttrValue  + " are " + blockNestedLoopsSky.getAllSkylineMembers().size());
            if (Phase3Utils.createMaterializedView(materTableName)) {
                Phase3Utils.insertIntoTable(blockNestedLoopsSky.getAllSkylineMembers(), materHeapfile);
            } else {
                Phase3Utils.printTuples(blockNestedLoopsSky.getAllSkylineMembers(), groupByAttrTypes);
            }
        } catch (Exception e) {
            System.out.println("Error occurred while Group By Skyline Computation!");
            e.printStackTrace();
        } finally {
            Phase3Utils.closeScan(scan);
        }
    }

}
