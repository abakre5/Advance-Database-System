package tests;

import bufmgr.PageNotReadException;
import catalog.*;
import global.AttrType;
import global.ExtendedSystemDefs;
import global.IndexType;
import global.SystemDefs;
import heap.*;
import iterator.*;
import org.w3c.dom.Attr;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Phase3Utils {

    public final static int SIZE_OF_INT = 3;
    public final static int SIZE_OF_STRING = 32;
    public final static String GROUP_BY_ATTR_STRING_INITIALIZER = "INITIALIZER_GROUP_BY_ATTR";

    private final static String INDEX_META_DATA_FILE = "INDEXMETADATAFILE";
    private final static AttrType[] attrTypesIndexMetaData =
            {new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};


    private final static short[] strSizesIndexMetaData = {SIZE_OF_STRING, SIZE_OF_STRING};


    public static void writeToDisk() {
        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertIntoTable(List<Tuple> skylineElementsOfEachGroup, Heapfile materHeapfile) throws HFDiskMgrException, InvalidTupleSizeException, HFException, IOException, InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {
        for (Tuple tuple : skylineElementsOfEachGroup) {
            materHeapfile.insertRecord(tuple.returnTupleByteArray());
        }
    }

    public static IteratorDesc getTableItr(String tableName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, FileScanException, TupleUtilsException, InvalidRelation, PredEvalException, JoinsException, FieldNumberOutOfBoundException, PageNotReadException, InvalidTypeException, WrongPermat, UnknowAttrType {
        int numAttr = 0;

        if (!Phase3Driver.isTableInDB(tableName)) {
            System.err.println("*** error: relation " + tableName + " not found in DB");
            return null;
        }
        RelDesc rec = new RelDesc();
        try {
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(tableName, rec);
            numAttr = rec.getAttrCnt();
            if (numAttr == 0) {
                System.err.println("*** error: catalog attribute count is 0 ");
                return null;
            }
        } catch (Exception e) {
            System.err.println("*** error: " + e);
            return null;
        }
        AttrType[] attrTypes = new AttrType[numAttr];
        for (int i = 0; i < attrTypes.length; ++i) {
            attrTypes[i] = new AttrType(AttrType.attrNull);
        }
        short[] strSizes = new short[numAttr];
        for (int i = 0;i < numAttr;i++) {
            strSizes[i] = Phase3Utils.SIZE_OF_STRING;
        }

        try {
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getTupleStructure(tableName, numAttr, attrTypes, strSizes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return new IteratorDesc(tableName, (short) numAttr, attrTypes, strSizes);
    }

    public static boolean aggListContainsStringAttr(FldSpec[] agg_list, AttrType[] type) {
        for (FldSpec attr : agg_list) {
            if (!(type[attr.offset - 1].attrType == AttrType.attrInteger || type[attr.offset - 1].attrType == AttrType.attrReal)) {
                return true;
            }
        }
        return false;
    }

    public static attrInfo[] getAttrInfoGroupBy(AttrType[] attrType, FldSpec groupByAttr, FldSpec[] aggList) {
        attrInfo[] attrs = new attrInfo[2];
        attrs[0] = new attrInfo();
        attrs[0].attrType = new AttrType(attrType[groupByAttr.offset - 1].attrType);
        attrs[0].attrName = "Col" + 0;
        attrs[0].attrLen = (attrType[groupByAttr.offset - 1].attrType == AttrType.attrInteger) ? Phase3Utils.SIZE_OF_INT : Phase3Utils.SIZE_OF_STRING;

        attrs[1] = new attrInfo();
        attrs[1].attrType = new AttrType(attrType[aggList[0].offset - 1].attrType);
        attrs[1].attrName = "Col" + 1;
        attrs[1].attrLen = (attrType[aggList[0].offset - 1].attrType == AttrType.attrInteger) ? Phase3Utils.SIZE_OF_INT : Phase3Utils.SIZE_OF_STRING;
        return attrs;
    }

    public static boolean createMaterializedView(String materTableName) {
        if (materTableName == "")
            return false;
        return true;
    }

    public static void createTable(String materTableName, int attrCnt, attrInfo[] attrs) {
        if (createMaterializedView(materTableName)) {
            try {
                ExtendedSystemDefs.MINIBASE_RELCAT.createRel(materTableName, attrCnt, attrs);
            } catch (IOException | Catalogmissparam | Catalogrelexists | Catalogdupattrs | Catalognomem | RelCatalogException | Catalogioerror | Cataloghferror catalogmissparam) {
                catalogmissparam.printStackTrace();
                System.err.println("**** Error occurred while creating table -> " + catalogmissparam.getMessage());
            }
        }
    }


    /**
     * @param tuple    Tuple
     * @param index    attribute index whose value is desired
     * @param attrType attribute type of the attribute whose value is required.
     * @return get attribute value for a tuple
     * @throws Exception
     */
    public static Float getAttrVal(Tuple tuple, int index, AttrType attrType) throws IOException, FieldNumberOutOfBoundException, TupleUtilsException {
        if (attrType.attrType == AttrType.attrInteger) {
            return (float) tuple.getIntFld(index);
        } else if (attrType.attrType == AttrType.attrReal) {
            return tuple.getFloFld(index);
        } else {
            throw new TupleUtilsException("String operation not supported");
        }
    }

    public static String getAttrValString(Tuple tuple, int index, AttrType attrType) throws IOException, FieldNumberOutOfBoundException, TupleUtilsException {
        if (attrType.attrType == AttrType.attrString) {
            return tuple.getStrFld(index);
        }  else {
            throw new TupleUtilsException("Float/Int operations not supported in this function");
        }
    }



    public static Tuple getAggTuple(float groupByAttrValue, int groupByAttr, float aggVal, AttrType aggAttrType, AttrType[] attrType) {
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
            if (aggAttrType.attrType == AttrType.attrReal) {
                t.setFloFld(2, aggVal);
            } else if (aggAttrType.attrType == AttrType.attrInteger) {
                t.setIntFld(2, (int) aggVal);
            } else {
                throw new TupleUtilsException("String is not supported as an agg list  attr");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return t;
    }



    public static Tuple getAggTupleGroupByAttrString(String groupByAttrValue, int groupByAttr, float aggVal, AttrType aggAttrType) {
        Tuple t = new Tuple();
        try {
            AttrType[] attrTypes = {new AttrType(AttrType.attrString), aggAttrType};
            short[] strSizes = new short[1];
            strSizes[0] = Phase3Utils.SIZE_OF_STRING;
            try {
                t.setHdr((short) 2, attrTypes, strSizes);
            } catch (Exception e) {
                e.printStackTrace();
            }

            t.setStrFld(1, groupByAttrValue);
            if (aggAttrType.attrType == AttrType.attrReal) {
                System.out.println("Adding float -> " +  aggVal);
                t.setFloFld(2, aggVal);
            } else if (aggAttrType.attrType == AttrType.attrInteger) {
                t.setIntFld(2, (int) aggVal);
            } else {
                throw new TupleUtilsException("String is not supported as an agg list  attr");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return t;
    }

    public static AttrType[] getGroupByAttrTypes(attrInfo[] attrs) {

        AttrType[] groupByAttr = new AttrType[attrs.length];
        for (int i = 0;i < attrs.length;i++) {
            groupByAttr[i] = attrs[i].attrType;
        }
        return groupByAttr;
    }

    public static void printTuples(List<Tuple> tuples, AttrType[] groupByAttrTypes) throws IOException {
        for (Tuple tuple : tuples) {
            tuple.print(groupByAttrTypes);
        }
    }

    public static void checkIndexesOnTable(String relName, int nFlds, int attr, int indexCnt, IndexDesc[] indexDescList) {
        AttrDesc[] attrDescs = new AttrDesc[nFlds];
        try {
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName, nFlds, attrDescs);
            String attrName = attrDescs[attr-1].attrName;
            ExtendedSystemDefs.MINIBASE_INDCAT.getAttrIndexes(relName, attrName, indexCnt, indexDescList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void insertIndexEntry(String relationName, int attrIndex, int indexTypeInt) {
        try {
            Heapfile file = new Heapfile(INDEX_META_DATA_FILE);
            Tuple tuple = new Tuple();
            tuple.setHdr((short) 3, attrTypesIndexMetaData, strSizesIndexMetaData);
            int size = tuple.size();
            tuple = new Tuple(size);
            tuple.setHdr((short) 3, attrTypesIndexMetaData, strSizesIndexMetaData);
            tuple.setStrFld(1, relationName);
            tuple.setIntFld(2, attrIndex);
            tuple.setStrFld(3, new IndexType(indexTypeInt).toString());
            file.insertRecord(tuple.returnTupleByteArray());
            System.out.println("Index inserted successfully in the catalog");
        } catch (Exception e) {
            System.err.println("*** Error occurred while inserting index to catalog");
        }

    }

    public static boolean isIndexExists(String relationName, int attrIndex, IndexType indexType) {
        FileScan indexMetaDataFileScan = null;
        boolean isIndex = false;
        try {
            FldSpec[] proj_list = new FldSpec[3];
            for (int i = 0; i < 3; i++) {
                proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

            indexMetaDataFileScan = new FileScan(INDEX_META_DATA_FILE, attrTypesIndexMetaData, strSizesIndexMetaData, (short) 3, 3, proj_list, null);
            Tuple tuple;
            while ((tuple = indexMetaDataFileScan.get_next()) != null) {
                if (tuple.getStrFld(1).equals(relationName) && tuple.getIntFld(2) == attrIndex && tuple.getStrFld(3).equals(indexType.toString())) {
                    isIndex = true;
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("*** Error occurred while search for index in index catalog");
            e.printStackTrace();
        } finally {
            if (indexMetaDataFileScan != null) {
                indexMetaDataFileScan.close();
            }
        }
        return isIndex;
    }
}
