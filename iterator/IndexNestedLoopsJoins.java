package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import catalog.*;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;
import index.IndexUtils;
import tests.Phase3Utils;

import java.io.File;
import java.io.IOException;

/**
 * This file contains an implementation of Index Nested Loops Join
 * @author rrgore
 */

public class IndexNestedLoopsJoins extends Iterator {

    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private Iterator outer;
    private short t1_str_sizescopy[];
    private short t2_str_sizescopy[];
    private CondExpr OutputFilter[];
    private CondExpr RightFilter[];
    private int n_buf_pgs;        // # of buffer pages available.
    private boolean done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple;           // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile hf;
    private IndexScan inner;
    private String _relationName;
    private NestedLoopsJoins nlj;
    private boolean isHash = false;

    /**
     * constructor
     * Initialize the two relations which are joined, including relation type,
     *
     * @param in1          Array containing field types of R.
     * @param len_in1      # of columns in R.
     * @param t1_str_sizes shows the length of the string fields.
     * @param in2          Array containing field types of S
     * @param len_in2      # of columns in S
     * @param t2_str_sizes shows the length of the string fields.
     * @param amt_of_mem   IN PAGES
     * @param am1          access method for left i/p to join
     * @param relationName access heapfile for right i/p to join
     * @param outFilter    select expressions
     * @param rightFilter  reference to filter applied on right i/p
     * @param proj_list    shows what input fields go where in the output tuple
     * @param n_out_flds   number of outer relation fileds
     * @throws IOException         some I/O fault
     * @throws NestedLoopException exception from this class
     */
    public IndexNestedLoopsJoins(AttrType in1[],
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
    ) throws IOException, IndexNestedLoopException, GetFileEntryException, NestedLoopException, Catalogindexnotfound, Catalogattrnotfound, Catalogmissparam, Cataloghferror, Catalognomem, IndexCatalogException, Catalogioerror, Catalogrelnotfound, AttrCatalogException {
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
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        } catch (TupleUtilsException e) {
            throw new IndexNestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
        }

        _relationName = relationName;
        try {
            hf = new Heapfile(relationName);
        } catch (Exception e) {
            throw new IndexNestedLoopException(e, "Create new heapfile failed.");
        }

        // TODO: Check if index present on join attr, if yes, check if BTree or Hash

        // Check if index is present on inner relation on given join attribute
        int joinFldInner = OutputFilter[0].operand2.symbol.offset;
        int indexCnt = 0;
        IndexDesc[] indexDescsList = null;
        Phase3Utils.checkIndexesOnTable(relationName, len_in2, joinFldInner, indexCnt, indexDescsList);

        if (indexCnt == 0) {
            nlj = new NestedLoopsJoins(in1, len_in1, t1_str_sizes, in2,
                    len_in2, t2_str_sizes, amt_of_mem, am1,
                    relationName, outFilter, rightFilter, proj_list, n_out_flds);
        } else {
            nlj = null;
            IndexDesc joinIndexDesc = indexDescsList[joinFldInner-1];
            System.out.println("Index on attr: "+indexDescsList);
            IndexType joinIndexType = joinIndexDesc.getAccessType();
            if (joinIndexType.indexType == IndexType.Hash) {
                isHash = true;
            }
        }

//        PageId headerPageId = get_file_entry("innerIndex");
//        if (headerPageId == null) //file not exist
//        {
//            nlj = new NestedLoopsJoins(in1, len_in1, t1_str_sizes, in2,
//                    len_in2, t2_str_sizes, amt_of_mem, am1,
//                    relationName, outFilter, rightFilter, proj_list, n_out_flds);
//        } else {
//            nlj = null;
//        }
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    /**
     * Creates a FileScan on the given relation
     * @param fileName              Name of the relation file
     * @param isOuter               Is relation inner or outer?
     * @return                      FileScan on the given relation
     * @throws IOException          exception from this class
     * @throws FileScanException    exception from this class
     * @throws TupleUtilsException  exception from this class
     * @throws InvalidRelation      exception from this class
     */
    private FileScan getFileScan(String fileName, boolean isOuter) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan;

        if( isOuter ) {
            FldSpec[] Pprojection = new FldSpec[in1_len];
            for (int i = 1; i <= in1_len; i++) {
                Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
            }
            scan = new FileScan(fileName, _in1, t1_str_sizescopy,
                    (short) in1_len, in1_len, Pprojection, null);
        } else {
            FldSpec[] Pprojection = new FldSpec[in2_len];
            for (int i = 1; i <= in2_len; i++) {
                Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
            }
            scan = new FileScan(fileName, _in2, t2_str_sizescopy,
                    (short) in2_len, in2_len, Pprojection, null);
        }

        return scan;
    }

    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if (nlj != null) {
            return nlj.get_next();
        } else {
            if (done)
                return null;

            RID rid = new RID();

            // Get tuple from inner using index
            int joinFldInner = OutputFilter[0].operand2.symbol.offset;
            int joinFldOuter = OutputFilter[0].operand1.symbol.offset;

            CondExpr[] selects = new CondExpr[2];
            selects[0] = new CondExpr();
            selects[0].op = new AttrOperator(AttrOperator.aopEQ);
            selects[0].type1 = new AttrType(AttrType.attrSymbol);
            selects[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), joinFldInner);
            selects[0].next = null;

            selects[1] = null;
            Tuple t = null;

            do {
                // If get_from_outer is true, Get a tuple from the outer, delete
                // an existing scan on the file, and reopen a new scan on the file.
                // If a get_next on the outer returns DONE?, then the nested loops
                //join is done too.

                if (get_from_outer) {
                    get_from_outer = false;

                    // Get next tuple from outer on given join condition
                    if ((outer_tuple = outer.get_next()) == null) {
                        done = true;

                        return null;
                    }

                    switch (_in2[joinFldInner - 1].attrType) {
                        case (AttrType.attrInteger) : {
                            selects[0].type2 = new AttrType(AttrType.attrInteger);
                            selects[0].operand2.integer = outer_tuple.getIntFld(joinFldOuter);
                            break;
                        }
                        case (AttrType.attrReal) : {
                            selects[0].type2 = new AttrType(AttrType.attrReal);
                            selects[0].operand2.real = outer_tuple.getFloFld(joinFldOuter);
                            break;
                        }
                        case (AttrType.attrString) : {
                            selects[0].type2 = new AttrType(AttrType.attrString);
                            selects[0].operand2.string = outer_tuple.getStrFld(joinFldOuter);
                            break;
                        }
                    }

                    FldSpec[] proj = new FldSpec[in2_len];
                    for (int i = 1; i <= in2_len; i++) {
                        proj[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
                    }

                    try {

                        inner = new IndexScan(new IndexType(IndexType.B_Index), _relationName,
                                "innerIndex", _in2, t2_str_sizescopy, in2_len, in2_len, proj,
                                selects, joinFldInner, false);
                    } catch (Exception e) {
                        throw new IndexNestedLoopException(e, "Cannot get next tuple");
                    }

                }  // ENDS: if (get_from_outer == TRUE)


                // The next step is to get a tuple from the inner,
                // while the inner is not completely scanned && there
                // is no match (with pred),get a tuple from the inner.


                try {
                    t = inner.get_next();

                    if (t != null) {
                        Projection.Join(outer_tuple, _in1, t, _in2, Jtuple, perm_mat, nOutFlds);
                        return Jtuple;
                    } else {
                        inner.close();
                        get_from_outer = true;
                    }

                } catch (Exception e) {
                    throw new IndexNestedLoopException(e, "Cannot get tuple from index");
                }

                // There has been no match. (otherwise, we would have
                //returned from t//he while loop. Hence, inner is
                //exhausted, => set get_from_outer = TRUE, go to top of loop
            } while (true);
        }
    }

    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {

            try {
                outer.close();
            } catch (Exception e) {
                throw new JoinsException(e, "IndexNestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
