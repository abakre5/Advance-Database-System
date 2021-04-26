package iterator;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import static global.AttrType.attrInteger;
import static global.AttrType.attrReal;

public class HashJoin5a extends Iterator {
    private Heapfile materialisedTable;
    private String materialisedTableName;
    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private Iterator outer;
    private short t2_str_sizescopy[];
    private short t1_str_sizescopy[];
    private CondExpr OutputFilter[];
    private CondExpr RightFilter[];
    private int n_buf_pgs;        // # of buffer pages available.
    private boolean done;         // Is the join complete
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple;           // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile hf;
    private Scan inner;
    private String relation;
    private Scan joinScan;
    FldSpec joinAttr1;
    FldSpec joinAttr2;
    FldSpec mergeAttr1;
    FldSpec mergeAttr2;
    AttrType[] Jtypes;
    private ArrayList<Integer> oHashList, iHashList;
    AttrType[] temp11;
    short[] temp11_res_str_sizes;
    RID temp11_RID;
    public HashJoin5a(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            FldSpec joinAttr1,
            FldSpec mergeAttr1,
            AttrType[] in2, int len_in2, short[] t2_str_sizes,
            FldSpec joinAttr2,
            FldSpec mergeAttr2,
            String relationName1,
            String relationName2,
            int k,
            int n_pages,
            String materialisedTableName) throws IOException, NestedLoopException, HashJoinException {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;
        this.joinAttr1 = joinAttr1;
        this.joinAttr2 = joinAttr2;
        this.mergeAttr1 = mergeAttr1;
        this.mergeAttr2 = mergeAttr2;
        if(!materialisedTableName.equals("")){
            try {
                this.materialisedTableName = materialisedTableName;
                this.materialisedTable = new Heapfile(materialisedTableName);
            } catch (HFException | HFBufMgrException | HFDiskMgrException e) {
                System.out.println("File creation for materialised file failed.");
                e.printStackTrace();
            }
        }


        try {
            outer = getFileScan(relationName1, (short) len_in1, in1, t1_str_sizes);
        } catch (FileScanException | TupleUtilsException | InvalidRelation e) {
            System.out.println("file scan creation failed");
            e.printStackTrace();
        }
        t1_str_sizescopy = t1_str_sizes;
        t2_str_sizescopy = t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        RightFilter = null;

        n_buf_pgs = n_pages;
        inner = null;
        done = false;

        short[] t_size;

        nOutFlds = len_in1 + len_in2;
        Jtypes = new AttrType[nOutFlds];
        relation = relationName2;
        joinScan = null;

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        setJoinSpecification(outFilter);
        OutputFilter = outFilter;

        FldSpec[] proj_list = new FldSpec[nOutFlds];
//        System.out.println("con=ming till here ");
        for (int i = 1; i <= len_in1; i++) {
            proj_list[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        for (int i = 1; i <= len_in2; i++) {
            proj_list[len_in1 + i - 1] = new FldSpec(new RelSpec(RelSpec.innerRel),  i);
        }
//        System.out.println("coming till here ");

        perm_mat = proj_list;

        try {
            hf = new Heapfile(relationName2);
        } catch (Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }

        if (false) {
            // TODO: Check for hash index on both tuples -> use them for HashJoin
        } else {
            try {
                createInnerHashPartition();
                createOuterHashPartition();
                ArrayList<ArrayList<Float>> topKCandidateIndexList = performHashJoin();
                ArrayList<ArrayList<Float>> topKIndexList = new ArrayList<>();
                topKCandidateIndexList.sort((o1, o2) -> o2.get(1).compareTo(o1.get(1)));
                int i = 1;
                for (ArrayList<Float> list: topKCandidateIndexList){
                    topKIndexList.add(list);
                    if (i == k){
                        break;
                    }
                    i++;
                }
                topKIndexList.sort(Comparator.comparing(o2 -> o2.get(0)));
                topKCandidateIndexList.clear();
                Heapfile topKResult;
                try {
                    topKResult = new Heapfile("TopKForHashJoin");
                } catch (Exception e) {
                    throw new HashJoinException(e, "Create new heapfile failed.");
                }
                FileScan fileScan = getFileScan("hashJoinFile.in", (short) (nOutFlds+1), temp11,  temp11_res_str_sizes);
                Tuple tuple = fileScan.get_next();
                int index = 0;
                int index1 = 0;
                while (tuple != null){
                    if(topKIndexList.get(index1).get(0) == index){
                        index1++;
                        topKResult.insertRecord(new Tuple(tuple).getTupleByteArray());
                    }
                    if (index1 == k){
                        break;
                    }
                    index++;
                    tuple = fileScan.get_next();
                }

                FileScan fileScan1 = getFileScan("TopKForHashJoin", (short) (nOutFlds+1), temp11,  temp11_res_str_sizes);
                Tuple tuple1 = fileScan1.get_next();

                System.out.println("************* Final Output**************");

                while (tuple1 != null){
                    if (materialisedTable!= null){
                        materialisedTable.insertRecord(tuple1.getTupleByteArray());
                        System.out.println("Adding to file");
                    } else {
                        tuple1.print(temp11);
                    }
                    tuple1 = fileScan1.get_next();
                }
                fileScan1.close();
                fileScan1.deleteFile();
                close();

            } catch (Exception e) {
                throw new HashJoinException(e, "Create new heapfile failed.");
            }
        }
    }

    private void setJoinSpecification(CondExpr[] expr) {
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), joinAttr1.offset );
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), joinAttr2.offset);

        expr[1] = null;
    }

    private void createInnerHashPartition() throws IOException, HFException, HFBufMgrException, HFDiskMgrException, HashJoinException {
        // Push all values in table into hash partitions after hashing
        Tuple data = null;
        RID rid = null;
        FileScan fscan = null;
        iHashList = new ArrayList<>();
        Heapfile bucketFile = null;

        FldSpec[] proj = new FldSpec[in2_len];
        for (int i = 1; i <= in2_len; i++) {
            //should this be outer ?
            proj[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }

        try {
            fscan = new FileScan(relation, _in2, t2_str_sizescopy, (short) in2_len, in2_len, proj, null);
            data = fscan.get_next();
            data.setHdr((short) in2_len, _in2, t2_str_sizescopy);
            int joinFldInner = OutputFilter[0].operand2.symbol.offset;
            int bucket=-1;
            String bucket_name = "";

            while(data != null) {
                switch (_in2[joinFldInner - 1].attrType) {
                    case AttrType.attrInteger:
                        Integer iVal = data.getIntFld(joinFldInner);
                        bucket = get_hash(iVal);
                        break;
                    case AttrType.attrReal:
                        Float rVal = data.getFloFld(joinFldInner);
                        bucket = get_hash(rVal);
                        break;
                    case AttrType.attrString:
                        String sVal = data.getStrFld(joinFldInner);
                        bucket = get_hash(sVal);
                        break;
                    default:
                        break;
                }

                bucket_name = "inner_hash_bucket_"+bucket;
                if (!iHashList.contains(bucket)) {
                    iHashList.add(bucket);
                }

                bucketFile = new Heapfile(bucket_name);
                rid = bucketFile.insertRecord(data.getTupleByteArray());
                data = fscan.get_next();
            }
        } catch(Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }
    }

    private void createOuterHashPartition() throws IOException, HFException, HFBufMgrException, HFDiskMgrException, HashJoinException {
        Tuple data = null;
        RID rid = null;
        FileScan fscan = null;
        oHashList = new ArrayList<>();
        Heapfile bucketFile = null;

        FldSpec[] proj = new FldSpec[in1_len];
        for (int i = 1; i <= in1_len; i++) {
            proj[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }

        try {
            data = outer.get_next();
            data.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            int joinFldOuter = OutputFilter[0].operand1.symbol.offset;
            int bucket=-1;
            String bucket_name = "";

            while(data != null) {
                switch (_in1[joinFldOuter - 1].attrType) {
                    case AttrType.attrInteger:
                        Integer iVal = data.getIntFld(joinFldOuter);
                        bucket = get_hash(iVal);
                        break;
                    case AttrType.attrReal:
                        Float rVal = data.getFloFld(joinFldOuter);
                        bucket = get_hash(rVal);
                        break;
                    case AttrType.attrString:
                        String sVal = data.getStrFld(joinFldOuter);
                        bucket = get_hash(sVal);
                        break;
                    default:
                        break;
                }

                bucket_name = "outer_hash_bucket_"+bucket;
                if (!oHashList.contains(bucket)) {
                    oHashList.add(bucket);
                }

                 bucketFile = new Heapfile(bucket_name);
                rid = bucketFile.insertRecord(data.getTupleByteArray());

                data = outer.get_next();
            }
        } catch(Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }
    }
    public void addToTopKCandidateIndexList(int index, float avg, ArrayList<ArrayList<Float>> topKCandidateIndexList) {
        ArrayList<Float> tmp = new ArrayList<>();
        tmp.add((float)index);
        tmp.add(avg);
        topKCandidateIndexList.add(tmp);
    }

    private ArrayList<ArrayList<Float>> performHashJoin() throws HashJoinException {
        // Pick corresponding buckets
        ArrayList<ArrayList<Float>> topKCandidateIndexList = new ArrayList<ArrayList<Float>>();
        int index = 0;
        Heapfile joinFile = null;
        Heapfile innerFile = null;
        Heapfile outerFile = null;
        FileScan outerScan;
        NestedLoopsJoins nlj;

        try {
            joinFile = new Heapfile("hashJoinFile.in");

            for (int hash : oHashList) {
                String innerFileName = "inner_hash_bucket_"+hash;
                innerFile = new Heapfile(innerFileName);
                String outerFileName = "outer_hash_bucket_"+hash;
                outerFile = new Heapfile(outerFileName);

                // Check if the buckets actually contain any tuples
                if(innerFile.getRecCnt() == 0 || outerFile.getRecCnt() == 0) {
                    continue;
                }

                // Perform NLJ
                FldSpec[] oProj = getProjection(in1_len);

                outerScan = new FileScan(outerFileName, _in1, t1_str_sizescopy, (short) in1_len, in1_len, oProj, null);

                // Perform NLJ
                nlj = new NestedLoopsJoins(_in1, in1_len, t1_str_sizescopy, _in2,
                        in2_len, t2_str_sizescopy, n_buf_pgs, outerScan,
                        innerFileName, OutputFilter, RightFilter, perm_mat,
                        nOutFlds);
                TupleRIDPair tupleRIDPair =  nlj.get_next1();

                while (tupleRIDPair != null) {
                    Tuple t = tupleRIDPair.getTuple();
//                    t.print(Jtypes);
                    float mergeAttributeValue1 = 0f;
                    float mergeAttributeValue2 = 0f;
                    AttrType y = _in1[mergeAttr1.offset - 1];
                    switch (y.attrType) {
                        case attrInteger:
                            mergeAttributeValue1 = t.getIntFld(mergeAttr1.offset);
                            break;
                        case attrReal:
                            mergeAttributeValue1 = t.getFloFld(mergeAttr1.offset);
                            break;
                    }
                    AttrType z = _in1[mergeAttr2.offset - 1];
                    switch (z.attrType) {
                        case attrInteger:
                            mergeAttributeValue2 = t.getIntFld(mergeAttr2.offset + in1_len);
                            break;
                        case attrReal:
                            mergeAttributeValue2 = t.getFloFld(mergeAttr2.offset + in1_len);
                            break;
                    }
                    addToTopKCandidateIndexList(index, (mergeAttributeValue2 + mergeAttributeValue1)/2 , topKCandidateIndexList);
                    index++;
                    Tuple tuple = addAverageFieldToTuple(t, (mergeAttributeValue2 + mergeAttributeValue1)/2);
//                    tuple.print(temp11);
                    joinFile.insertRecord(tuple.getTupleByteArray());
                    tupleRIDPair = nlj.get_next1();
                }
            }
        } catch (Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }
        return topKCandidateIndexList;
    }

    private Tuple addAverageFieldToTuple(Tuple inputTuple, float avg) throws IOException, TupleUtilsException, FieldNumberOutOfBoundException {

        AttrType[] temp = new AttrType[nOutFlds+1];
        Tuple resultTuple = new Tuple();
        setup_op_tuple_extra_field(resultTuple, temp,
                _in1, in1_len, _in2, in2_len,
                t1_str_sizescopy, t2_str_sizescopy,
                perm_mat, (nOutFlds+1), new AttrType(AttrType.attrReal));

        for (int i =0; i< nOutFlds ; i++){
            if (temp[i].attrType == AttrType.attrInteger){
                resultTuple.setIntFld(i+1 , inputTuple.getIntFld(i+1));
            } else if (temp[i].attrType == AttrType.attrReal){
                resultTuple.setFloFld(i+1 , inputTuple.getFloFld(i+1));
            } else if (temp[i].attrType == AttrType.attrString){
                resultTuple.setStrFld(i+1 , inputTuple.getStrFld(i+1));
            }
        }
        resultTuple.setFloFld(nOutFlds+1,avg);
        temp11 = temp;
//        resultTuple.print(temp);
        return resultTuple;
    }

    public int get_hash(Object value) {
        return value.hashCode();
    }

    private FldSpec[] getProjection(int numFlds) {
        FldSpec[] proj = new FldSpec[numFlds];
        for (int j = 1; j <= numFlds; j++) {
            proj[j - 1] = new FldSpec(new RelSpec(RelSpec.outer), j);
        }
        return proj;
    }

    private void initJoinScan() {
        Heapfile joinFile = null;
        try {
            joinFile = new Heapfile("hashJoinFile.in");
            joinScan = new Scan(joinFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void closeBucketFiles() throws HashJoinException {
        Heapfile f = null;
        try {
            for (int hash : iHashList) {
                String fileName = "inner_hash_bucket_"+hash;
                f = new Heapfile(fileName);
                f.deleteFile();
            }

            for (int hash : oHashList) {
                String fileName = "outer_hash_bucket_"+hash;
                f = new Heapfile(fileName);
                f.deleteFile();
            }
            String fileName = "hashJoinFile.in";
            f = new Heapfile(fileName);
            f.deleteFile();
        } catch (Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if (done) {
            return null;
        }

        if (joinScan == null) {
            initJoinScan();
        }

        RID rid = new RID();
        Jtuple = joinScan.getNext(rid);
        if (Jtuple == null) {
            done = true;
            return null;
        }

        return Jtuple;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {
            try {
                outer.close();
                closeBucketFiles();
            } catch (Exception e) {
                throw new JoinsException(e, "HashJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
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
    private FileScan getFileScan(String relationName, short noOfColumns, AttrType[] attrTypes, short[] stringSizes) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
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
     * set up the Jtuple's attrtype, string size,field number for using join
     *
     * @param Jtuple       reference to an actual tuple  - no memory has been malloced
     * @param res_attrs    attributes type of result tuple
     * @param in1          array of the attributes of the tuple (ok)
     * @param len_in1      num of attributes of in1
     * @param in2          array of the attributes of the tuple (ok)
     * @param len_in2      num of attributes of in2
     * @param t1_str_sizes shows the length of the string fields in S
     * @param t2_str_sizes shows the length of the string fields in R
     * @param proj_list    shows what input fields go where in the output tuple
     * @param nOutFlds     number of outer relation fileds
     * @throws IOException         some I/O fault
     * @throws TupleUtilsException exception from this class
     */
    public short[] setup_op_tuple_extra_field(Tuple Jtuple, AttrType[] res_attrs,
                                         AttrType in1[], int len_in1, AttrType in2[],
                                         int len_in2, short t1_str_sizes[],
                                         short t2_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds, AttrType extraAttrType )
            throws IOException,
            TupleUtilsException {
        short[] sizesT1 = new short[len_in1];
        short[] sizesT2 = new short[len_in2];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        for (count = 0, i = 0; i < len_in2; i++)
            if (in2[i].attrType == AttrType.attrString)
                sizesT2[i] = t2_str_sizes[count++];
        int n_strs = 0;
        for (i = 0; i < nOutFlds-1; i++) {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);
            else if (proj_list[i].relation.key == RelSpec.innerRel)
                res_attrs[i] = new AttrType(in2[proj_list[i].offset - 1].attrType);
        }
        res_attrs[nOutFlds-1] = new AttrType(extraAttrType.attrType);

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds-1; i++) {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                n_strs++;
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short[n_strs];
        count = 0;
        for (i = 0; i < nOutFlds-1; i++) {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
                res_str_sizes[count++] = sizesT2[proj_list[i].offset - 1];
        }
        try {
            temp11_res_str_sizes = res_str_sizes;
            Jtuple.setHdr((short) nOutFlds, res_attrs, res_str_sizes);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "setHdr() failed");
        }
        return res_str_sizes;
    }

}
