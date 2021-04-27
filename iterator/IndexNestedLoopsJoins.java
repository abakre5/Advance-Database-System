package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import catalog.*;
import global.*;
import hash.HashFile;
import hash.IntegerKey;
import hash.StringKey;
import heap.*;
import index.IndexException;
import index.IndexScan;
import tests.Phase3Utils;
import tests.TableIndexDesc;

import java.io.IOException;
import java.util.List;

import static tests.Phase3Utils.*;

/**
 * This file contains an implementation of Index Nested Loops Join
 * @author rrgore
 */
public class IndexNestedLoopsJoins extends Iterator {

    private AttrType outerTypes[], innerTypes[];
    private int nOuterRecs, nInnerRecs;
    private Iterator outer;
    private short outerStrSizes[];
    private short innerStrSizes[];
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
    private AttrType[] Jtypes;
    private String indexFileName;
    private List<RID> innerRidList;

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
     * @throws IndexNestedLoopException exception from this class
     * @throws NestedLoopException exception from Nested Loop Join
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
    ) throws IOException, IndexNestedLoopException, NestedLoopException {
        outerTypes = new AttrType[in1.length];
        innerTypes = new AttrType[in2.length];
        System.arraycopy(in1, 0, outerTypes, 0, in1.length);
        System.arraycopy(in2, 0, innerTypes, 0, in2.length);
        nOuterRecs = len_in1;
        nInnerRecs = len_in2;

        outer = am1;
        outerStrSizes = t1_str_sizes;
        innerStrSizes = t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter = rightFilter;

        n_buf_pgs = amt_of_mem;
        inner = null;
        done = false;
        get_from_outer = true;

        Jtypes = new AttrType[n_out_flds];
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

        int joinFldInner = OutputFilter[0].operand2.symbol.offset;
        List<TableIndexDesc> indexList = getIndexesOnTable(_relationName);
        if (indexList.isEmpty()) {
            nlj = new NestedLoopsJoins(in1, len_in1, t1_str_sizes, in2,
                    len_in2, t2_str_sizes, amt_of_mem, am1,
                    relationName, outFilter, rightFilter, proj_list, n_out_flds);
        } else {
            isHash = true;
            boolean isJoinAttr = false; // Checks if index is on the join attribute
            for (TableIndexDesc desc : indexList) {
                if (desc.getAttributeIndex() == joinFldInner) {
                    isJoinAttr = true;

                    // Go with B-index if possible, since it supports duplicates
                    if (desc.getType() == IndexType.B_Index) {
                        isHash = false;
                    }
                }
            }

            if (!isJoinAttr) {
                nlj = new NestedLoopsJoins(in1, len_in1, t1_str_sizes, in2,
                        len_in2, t2_str_sizes, amt_of_mem, am1,
                        relationName, outFilter, rightFilter, proj_list, n_out_flds);
            } else {
                nlj = null;
                if (!isHash) {
                    indexFileName = Phase3Utils.getUnClusteredBtreeIndexName(_relationName, joinFldInner);
                } else {
                    indexFileName = Phase3Utils.getUnclusteredHashIndexName(_relationName, joinFldInner);
                }
            }
        }
    }

    /**
     * @return The joined tuple is returned
     * @throws IOException               I/O errors
     * @throws JoinsException            some join exception
     * @throws IndexException            exception from super class
     * @throws InvalidTupleSizeException invalid tuple size
     * @throws InvalidTypeException      tuple type not valid
     * @throws PageNotReadException      exception from lower layer
     * @throws TupleUtilsException       exception from using tuple utilities
     * @throws PredEvalException         exception from PredEval class
     * @throws SortException             sort exception
     * @throws LowMemException           memory error
     * @throws UnknowAttrType            attribute type unknown
     * @throws UnknownKeyTypeException   key type unknown
     * @throws Exception                 other exceptions
     */
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if (nlj != null) {
            return nlj.get_next();
        } else if (isHash) {
            return hashGetNext();
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

                    switch (innerTypes[joinFldInner - 1].attrType) {
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

                    FldSpec[] proj = new FldSpec[nInnerRecs];
                    for (int i = 1; i <= nInnerRecs; i++) {
                        proj[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
                    }

                    try {

                        inner = new IndexScan(new IndexType(IndexType.B_Index), _relationName,
                                indexFileName, innerTypes, innerStrSizes, nInnerRecs, nInnerRecs, proj,
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
                        Projection.Join(outer_tuple, outerTypes, t, innerTypes, Jtuple, perm_mat, nOutFlds);
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

    /**
     * Gets next joined tuple when hash index is present on the inner relation
     *
     * @return The joined tuple is returned
     * @throws Exception other exceptions
     */
    public Tuple hashGetNext() throws Exception {
        if (done)
            return null;

        RID rid = new RID();

        // Get tuple from inner using index
        int joinFldInner = OutputFilter[0].operand2.symbol.offset;
        int joinFldOuter = OutputFilter[0].operand1.symbol.offset;

        Heapfile heapf = null;
        int num_records = 0;
        try {
            heapf = new Heapfile(_relationName);
            num_records = heapf.getRecCnt();
        } catch (Exception e) {
            throw new IndexNestedLoopException(e, "Create heapfile failed");
        }

        Tuple t = null;

        HashFile hashf = null;
        Scan scan = null;
        hash.KeyClass key = null;


        try {
            hashf = new HashFile(_relationName, indexFileName, joinFldInner, innerTypes[joinFldInner - 1].attrType,
                    num_records, heapf, innerTypes, innerStrSizes, nInnerRecs);

        } catch (Exception e) {
            throw new IndexNestedLoopException(e, "Cannot get next tuple");
        }

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

                switch (outerTypes[joinFldOuter - 1].attrType) {
                    case (AttrType.attrInteger) : {
                        key = new IntegerKey(outer_tuple.getIntFld(joinFldOuter));
                        break;
                    }
                    case (AttrType.attrString) : {
                        key = new StringKey(outer_tuple.getStrFld(joinFldOuter));
                        break;
                    }
                }

            }  // ENDS: if (get_from_outer == TRUE)


            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.


            try {
                // perform searchIndex to get RIDs of corresponding inner tuples
                if (innerRidList == null) {
                    innerRidList = hashf.searchHashIndexForJoin(key);
                }

                if (innerRidList.isEmpty()) {
                    get_from_outer = true;
                    innerRidList = null;
                } else {
                    RID inRid = innerRidList.get(0);

                    // get inner tuple using this RID
                    Tuple tt = new Tuple();
                    tt.setHdr((short) nInnerRecs, innerTypes, innerStrSizes);

                    int size = tt.size();

                    t = new Tuple(size);
                    t.setHdr((short) nInnerRecs, innerTypes, innerStrSizes);

                    tt = heapf.getRecord(inRid);
                    t.tupleCopy(tt);

                    innerRidList.remove(0);
                    if (innerRidList.isEmpty()) {
                        get_from_outer = true;
                        innerRidList = null;
                    }

                    if (t != null) {
                        Projection.Join(outer_tuple, outerTypes, t, innerTypes, Jtuple, perm_mat, nOutFlds);
                        return Jtuple;
                    }
                }

            } catch (Exception e) {
                throw new IndexNestedLoopException(e, "Cannot get tuple from index");
            }

            // There has been no match. (otherwise, we would have
            //returned from t//he while loop. Hence, inner is
            //exhausted, => set get_from_outer = TRUE, go to top of loop
        } while (true);
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     *
     * @throws IOException    I/O error from lower layers
     * @throws JoinsException join error from lower layers
     * @throws IndexException index access error
     * @throws SortException error while sorting
     */
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
