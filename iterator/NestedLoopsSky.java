package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created on 2/21/2021
 * @author Rahul
 */
public class NestedLoopsSky extends Iterator {
    private AttrType[] in1;
    private int n_buf_pgs;
    private Iterator outer;
    private int[] pref_list;
    private List<Tuple> skyline;
    private boolean first_time;

    public NestedLoopsSky(AttrType[] _in1,
                          int amt_of_mem,
                          Iterator am1,
                          int[] _pref_list
    ) throws IOException,NestedLoopException {
        in1 = _in1;
        n_buf_pgs = amt_of_mem;
        outer = am1;
        pref_list = _pref_list;
        first_time = true;
    }

    /**
     * Steps:
     * (1) Create a ArrayList of the scanned tuples
     * (2) Run 2 loops, drop elements that are not eligible for skyline
     * (3) Return the remaining value
     */
    private void setSkyline() throws Exception {
        Tuple currTuple = outer.get_next();
        List<Tuple> tuplesList = new ArrayList<>();
        while (currTuple != null) {
            tuplesList.add( new Tuple(currTuple) );
            currTuple = outer.get_next();
        }

        if( tuplesList.size() == 0 ) {
            skyline = Collections.emptyList();
            return;
        }

        skyline = new ArrayList<>();

        if( tuplesList.size() > 1 ) {
            for( int i=0; i<tuplesList.size(); i++ ) {
                for( int j=i+1; j<tuplesList.size(); j++ ) {
                    if( TupleUtils.Dominates( tuplesList.get(i), in1, tuplesList.get(j), in1, (short)in1.length, null, pref_list, pref_list.length ) ) {
                        tuplesList.remove( j );
                    } else if( TupleUtils.Dominates( tuplesList.get(j), in1, tuplesList.get(i), in1, (short)in1.length, null, pref_list, pref_list.length )) {
                        tuplesList.remove( i );
                        break;
                    }
                }
            }
        }

        skyline = tuplesList;
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if( first_time ) {
            first_time = false;
            setSkyline();
        }
        if( !skyline.isEmpty() ) {
            Tuple nextTuple = skyline.get(0);
            skyline.remove( 0 );
            return nextTuple;
        }
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {
            try {
                outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}

