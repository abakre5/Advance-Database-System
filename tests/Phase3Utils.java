package tests;

import btree.BTreeFile;
import bufmgr.PageNotReadException;
import catalog.*;
import diskmgr.PageCounter;
import global.*;
import heap.*;
import index.IndexScan;
import iterator.*;
import org.w3c.dom.Attr;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static tests.Phase3Driver.STR_SIZE;


/**
 * @author Abhishek Bakare
 */
public class Phase3Utils {

    public final static int SIZE_OF_INT = 3;
    public final static int SIZE_OF_STRING = 32;
    public final static String GROUP_BY_ATTR_STRING_INITIALIZER = "INITIALIZER_GROUP_BY_ATTR";

    private final static String INDEX_META_DATA_FILE = "IMDeF";
    private final static AttrType[] attrTypesIndexMetaData =
            {new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};


    private final static short[] strSizesIndexMetaData = {SIZE_OF_STRING};


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
        attrs[0].attrName = "Name";
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
            short[] strSizes = {Phase3Utils.SIZE_OF_STRING};
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
            tuple.setIntFld(3, indexTypeInt);
            file.insertRecord(tuple.returnTupleByteArray());
            System.out.println("Index inserted successfully in the catalog");
        } catch (Exception e) {
            System.err.println("*** Error occurred while inserting index to catalog");
        }

    }

    public static boolean isIndexExists(String relationName, int attrIndex, int indexType) {
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
                if (tuple.getStrFld(1).equals(relationName) && tuple.getIntFld(2) == attrIndex && tuple.getIntFld(3) == indexType) {
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

    public static List<TableIndexDesc> getIndexesOnTable(String relationName) {
        FileScan indexMetaDataFileScan = null;
        List<TableIndexDesc> indexes = new ArrayList<>();
        try {
            FldSpec[] proj_list = new FldSpec[3];
            for (int i = 0; i < 3; i++) {
                proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

            indexMetaDataFileScan = new FileScan(INDEX_META_DATA_FILE, attrTypesIndexMetaData, strSizesIndexMetaData, (short) 3, 3, proj_list, null);
            Tuple tuple;
            while ((tuple = indexMetaDataFileScan.get_next()) != null) {
                if (tuple.getStrFld(1).equals(relationName)) {
                    indexes.add(new TableIndexDesc(tuple.getIntFld(3), tuple.getIntFld(2)));
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
        return indexes;
    }


    public static boolean deleteIndexFromCatalog(String relationName, int attrIndex, int indexType) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        FileScan indexMetaDataFileScan = null;
        List<RID> indexToDelete = new ArrayList<>();
        boolean isIndexDeleted = false;
        try {
            FldSpec[] proj_list = new FldSpec[3];
            for (int i = 0; i < 3; i++) {
                proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }

            indexMetaDataFileScan = new FileScan(INDEX_META_DATA_FILE, attrTypesIndexMetaData, strSizesIndexMetaData, (short) 3, 3, proj_list, null);
            TupleRIDPair tupleRIDPair;
            while ((tupleRIDPair = indexMetaDataFileScan.get_next1()) != null) {
                Tuple tuple = tupleRIDPair.getTuple();
                if (tuple.getStrFld(1).equals(relationName) && tuple.getIntFld(2) == attrIndex && tuple.getIntFld(3) == indexType) {
                    indexToDelete.add(new RID(tupleRIDPair.getRID().pageNo, tupleRIDPair.getRID().slotNo));
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

        if (indexToDelete.size() > 0) {
            Heapfile file = new Heapfile(INDEX_META_DATA_FILE);
            for (RID rid : indexToDelete) {
                try {
                    isIndexDeleted = true;
                    file.deleteRecord(rid);
                } catch (Exception e) {
                    System.err.println("Error while deleting an index!");
                }
            }
        }

        return isIndexDeleted;
    }


    public static String getUnclusteredHashIndexName(String tableName, int attrNo)
    {
        String indexName = tableName + IndexType.getStringForType(IndexType.Hash) + Integer.toString(attrNo);
        return indexName;
    }
    
    public static String getUnclusteredHashHeapName(String tableName, int attrNo)
    {
        String heapFileName = tableName + IndexType.getStringForType(IndexType.Hash) + Integer.toString(attrNo) + "_data";
        return heapFileName;
    }

    public static String getClusteredBtreeIndexName(String tableName, int attrNo)
    {
        String indexName = tableName + IndexType.getStringForType(IndexType.B_ClusteredIndex) + Integer.toString(attrNo);
        return indexName;
    }

    public static String getUnClusteredBtreeIndexName(String tableName, int attrNo)
    {
        String indexName = tableName + IndexType.getStringForType(IndexType.B_Index) + Integer.toString(attrNo);
        return indexName;
    }

    public static String getClusteredBtreeHeapName(String tableName, int attrNo)
    {
        String heapFileName = tableName + IndexType.getStringForType(IndexType.B_ClusteredIndex) + Integer.toString(attrNo) + "_d";
        return heapFileName;
    }

    public static Heapfile sortHeapFile(String fileToSort, String sortedFile, int sortAttr,
                                        AttrType[] attrTypes, short[] strSizes, TupleOrder order, int recordlen)
            throws IOException, FileScanException, TupleUtilsException, InvalidRelation, SortException
    {
        FldSpec[] projections = new FldSpec[attrTypes.length];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int fieldNumber = 1; fieldNumber <= attrTypes.length; fieldNumber++)
        {
            projections[fieldNumber-1] = new FldSpec(rel, fieldNumber);
        }

        //TODO:Pawan check if this needs to be updated according to data
        int SORTPGNUM = 100;
        Heapfile hfSorted = getHeapFileInstance(sortedFile);

        FileScan fileScan = new FileScan(fileToSort, attrTypes, strSizes, (short)attrTypes.length, attrTypes.length,
                    projections, null);

        Sort sortIterator = new Sort(attrTypes, (short) attrTypes.length, strSizes, fileScan, sortAttr, order, recordlen, SORTPGNUM);

        Tuple t = null;
        try
        {
            t = sortIterator.get_next();
            while (t!= null)
            {
                Tuple temp = new Tuple(t.getTupleByteArray(), t.getOffset(), t.getLength());
                temp.setHdr((short) attrTypes.length, attrTypes, strSizes);
                hfSorted.insertRecord(temp.getTupleByteArray());
                t= sortIterator.get_next();
            }
        } catch(Exception e)
        {
            e.printStackTrace();
        }

        sortIterator.close();
        return hfSorted;
    }

    public static Heapfile getHeapFileInstance(String heapfileName)
    {
        Heapfile hf = null;
        try
        {
            hf = new Heapfile(heapfileName);
        } catch (Exception e) {
            System.err.println("*** Failed to heap file instance ***");
            e.printStackTrace();
        }

        return hf;
    }

    public static IndexScan getBtreeClusteredIndexScan(String relationName, AttrType[] attrTypes, short[] attrSizes, int indexField){

        String indexFile  = Phase3Utils.getClusteredBtreeIndexName(relationName, indexField);
        IndexScan indexScan = null;

        FldSpec[] projections = new FldSpec[attrTypes.length];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int fieldNumber = 1; fieldNumber <= attrTypes.length; fieldNumber++)
        {
            projections[fieldNumber-1] = new FldSpec(rel, fieldNumber);
        }

        try {
            indexScan = new IndexScan(new IndexType(IndexType.B_ClusteredIndex), null, indexFile, attrTypes, attrSizes, attrTypes.length,attrTypes.length, projections,null,indexField,false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return indexScan;
    }

    public static void createOutputTable(String outputTable, String[] fieldNames, AttrType[] jtypes, int nJoinAttr) throws Exception {
        try {
            int SIZE_OF_INT = 8;
            attrInfo[] attrs = new attrInfo[nJoinAttr];

            for (int i = 0; i < nJoinAttr; ++i) {
                attrs[i] = new attrInfo();
                attrs[i].attrType = new AttrType(jtypes[i].attrType);
                attrs[i].attrName = fieldNames[i];
                attrs[i].attrLen = (jtypes[i].attrType == AttrType.attrInteger || jtypes[i].attrType == AttrType.attrReal) ? SIZE_OF_INT : STR_SIZE;
            }
            ExtendedSystemDefs.MINIBASE_RELCAT.createRel(outputTable, nJoinAttr, attrs);
        } catch (Exception e) {
            throw new Exception("Create output table failed: ", e);
        }
    }

    public static BTreeFile getBtreeIndexFileForAttribute(String tableName, int attributeNum){

        String indexName = getUnClusteredBtreeIndexName(tableName,attributeNum);
        BTreeFile btf = null;
        try{
            btf = new BTreeFile(indexName);
        }catch (Exception e){
            e.printStackTrace();
        }
        return btf;
    }

    public static void closeScan(FileScan scan) {
        if (scan != null) {
            scan.close();
        }
    }

    public static String getClusteredHashIndexName(String RelationName, int KeyAttrIdx) {
        return RelationName + IndexType.getStringForType(IndexType.Clustered_Hash) + Integer.toString(KeyAttrIdx);
    }

    public static void printPageCount() {
        System.out.println("Reads: " + PageCounter.getReadCounter());
        System.out.println("Write: " + PageCounter.getWriteCounter());
    }
}
