package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashJoin extends Iterator {
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
    private ArrayList<Integer> oHashList, iHashList;


    public HashJoin(AttrType in1[],
                    int len_in1,
                    short t1_str_sizes[],
                    AttrType in2[],
                    int len_in2,
                    short t2_str_sizes[],
                    int amt_of_mem,
                    Iterator am1,
                    String relationName,
                    CondExpr outFilter[],
                    CondExpr rightFilter[],
                    FldSpec proj_list[],
                    int n_out_flds
    ) throws IOException, NestedLoopException, HashJoinException {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;


        outer = am1;
        t1_str_sizescopy = t1_str_sizes;
        t2_str_sizescopy = t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter = rightFilter;

        n_buf_pgs = amt_of_mem;
        inner = null;
        done = false;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        relation = relationName;
        joinScan = null;

        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        } catch (TupleUtilsException e) {
            throw new HashJoinException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
        }

        try {
            hf = new Heapfile(relationName);
        } catch (Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }

        if (false) {
            // TODO: Check for hash index on both tuples -> use them for HashJoin
        } else {
            try {
                createInnerHashPartition();
                createOuterHashPartition();
                performHashJoin();
            } catch (Exception e) {
                throw new HashJoinException(e, "Create new heapfile failed.");
            }
        }
    }

    private void createInnerHashPartition() throws IOException, HFException, HFBufMgrException, HFDiskMgrException, HashJoinException {
        // Push all values in table into hash partitions after hashing
        Tuple data = null;
        RID rid = null;
        FileScan fscan = null;
        iHashList = new ArrayList<>();

        FldSpec[] proj = new FldSpec[in2_len];
        for (int i = 1; i <= in2_len; i++) {
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

                Heapfile bucketFile = new Heapfile(bucket_name);
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

                Heapfile bucketFile = new Heapfile(bucket_name);
                rid = bucketFile.insertRecord(data.getTupleByteArray());

                data = outer.get_next();
            }
        } catch(Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }
    }

    private void performHashJoin() throws HashJoinException {
        // Pick corresponding buckets
        Heapfile joinFile = null;
        try {
            joinFile = new Heapfile("hashJoinFile.in");

            for (int hash : oHashList) {
                String innerFileName = "inner_hash_bucket_"+hash;
                Heapfile innerFile = new Heapfile(innerFileName);
                String outerFileName = "outer_hash_bucket_"+hash;
                Heapfile outerFile = new Heapfile(outerFileName);

                // Check if the buckets actually contain any tuples
                if(innerFile.getRecCnt() == 0 || outerFile.getRecCnt() == 0) {
                    continue;
                }

                // Perform NLJ
                FldSpec[] oProj = getProjection(in1_len);

                FileScan outerScan = new FileScan(outerFileName, _in1, t1_str_sizescopy, (short) in1_len, in1_len, oProj, null);

                // Perform NLJ
                NestedLoopsJoins nlj = new NestedLoopsJoins(_in1, in1_len, t1_str_sizescopy, _in2,
                        in2_len, t2_str_sizescopy, n_buf_pgs, outerScan,
                        innerFileName, OutputFilter, RightFilter, perm_mat,
                        nOutFlds);

                Jtuple = nlj.get_next();
                while (Jtuple != null) {
                    joinFile.insertRecord(Jtuple.getTupleByteArray());
                    Jtuple = nlj.get_next();
                }
            }
        } catch (Exception e) {
            throw new HashJoinException(e, "Create new heapfile failed.");
        }
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
}
