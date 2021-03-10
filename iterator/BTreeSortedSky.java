package iterator;

import btree.BTreeFile;
import btree.IndexFile;
import btree.IndexFileScan;
import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;


import java.io.IOException;
import java.util.ArrayList;
import java.util.*;
import java.util.Collection;
import java.util.Collections;
import java.util.List;



public class BTreeSortedSky extends Iterator {
    private AttrType[] attrTypes;
    private int len_in;
    private short[] str_sizes;
    private int memory;
    private int n_buf_pgs;
    private Iterator left_itr;
    private int[] pref_list;
    private List<Tuple> skyline;
    private IndexFile[] index_files;
    private int[] pref_lengths;
    private java.lang.String relation_name;
    private boolean first_time;
    private java.util.Iterator skyline_iterator;
    private NestedLoopsSky bnlskyline;

   public BTreeSortedSky(AttrType[] attrs, int len_attr, short[] attr_size,
             int amt, Iterator left, java.lang.String
                     relation, int[] pref_list1, int[] pref_lengths_list,
             IndexFile[] index_file_list,
             int n_pages1
    ) throws IOException,NestedLoopException {
        System.out.println("==================Here==================");
        attrTypes = attrs;
        len_in = len_attr;
        str_sizes = attr_size;
        memory = amt;
        left_itr = left;
        relation_name = relation;
        pref_list = pref_list1;
        pref_lengths = pref_lengths_list;
        index_files = index_file_list;
        n_buf_pgs = n_pages1;
        first_time = true;
    }

/*
    Steps:
   
 */
    private void compute_skyline() throws Exception
    {
        // start index scan
        //System.out.println("compute_skyline");
        IndexScan index_scan = null;
        ArrayList<Tuple> tuple_list = new ArrayList<Tuple>();
        ArrayList<Tuple> skyline_list = new ArrayList<Tuple>();

        FldSpec[] projections = new FldSpec[3];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projections[0] = new FldSpec(rel, 1);
        projections[1] = new FldSpec(rel, 2);
        projections[2] = new FldSpec(rel, 3);

       

            java.lang.String index_file_name = ((BTreeFile) index_files[0]).getDbname();
            // start index scan on index of first preference attribute
            try
            {
                index_scan = new IndexScan(new IndexType(IndexType.B_Index), relation_name, index_file_name, attrTypes, str_sizes, 3, 3, projections, null, 3, false);
            } catch (Exception e) {
                //  status = FAIL;
                e.printStackTrace();
            }
            System.out.println("compute_skyline: index scan created");
            //debug_printIndex(index_list[index_count], pref_list[index_count]);
        
        Tuple w = null;

        try {
            w = index_scan.get_next();
        } catch (Exception e) {
        
            e.printStackTrace();
        }
       // System.out.println(t.getFloFld(1) + " " + t.getFloFld(2) + " " + t.getFloFld(3));
        
        Tuple t = new Tuple(w);

        while(t!=null) {
           
            tuple_list.add(new Tuple(t));
            t = index_scan.get_next();
        }

        System.out.println("Size of tuple_list "+ tuple_list.size());
        for(Tuple p: tuple_list) {
            System.out.println(p.getFloFld(1) + " : " + p.getFloFld(2) + " : " + p.getFloFld(3));
        }
        System.out.println("Size of tuple_list "+ tuple_list.size());
        //Start skyline computation

        for (Tuple currentTuple : tuple_list) {
            System.out.println(currentTuple.getFloFld(1) + " " + currentTuple.getFloFld(2) + " " + currentTuple.getFloFld(3));
            if (is_skyline_candidate(currentTuple, skyline_list)) {
                skyline_list.add(new Tuple(currentTuple));
            }
        }
        
        System.out.println("================ Printing Skyline =======================\n");
        for(Tuple p: skyline_list) {
            System.out.println(p.getFloFld(1) + " : " + p.getFloFld(2) + " : " + p.getFloFld(3));
        }

        System.out.println("================ End Skyline =======================\n");
        //System.out.println(tuple_list);
        //generate_skyline(tuple_list);
    }

    private boolean is_skyline_candidate(Tuple curr_tuple, ArrayList<Tuple> skyline_list) throws IOException, TupleUtilsException {
        for (Tuple tupleInSkyline : skyline_list) {
            if (TupleUtils.Dominates(tupleInSkyline, attrTypes, curr_tuple, attrTypes, (short)attrTypes.length, str_sizes, pref_list, pref_list.length)) {
                return false;
            }
        }
        return true;
    }


    private void generate_skyline(List<Tuple> playersList)
    {
        System.out.println(playersList);

        RID rid;
        Heapfile f = null;
        java.lang.String heap_file_name = relation_name + "_newww";
        try
        {
            f = new Heapfile("heap_file_name");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }


        for (int i = 0; i <  playersList.size(); i++)
        {
            try
            {
                Tuple test1 = playersList.get(i);
                System.out.println("*** Tuple len ***" + test1.getFloFld(1) + test1.getFloFld(2) + test1.getFloFld(2) );
              
                byte[] test = playersList.get(i).returnTupleByteArray();
                System.out.println("*** Tuple len ***" + test.length);

                rid = f.insertRecord(playersList.get(i).returnTupleByteArray());
            }
            catch (Exception e)
            {
                System.err.println("*** error in Heapfile.insertRecord() ***");

                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }

        FldSpec[] projections = new FldSpec[3];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projections[0] = new FldSpec(rel, 1);
        projections[1] = new FldSpec(rel, 2);
        projections[2] = new FldSpec(rel, 3);

        // Scan the players table
        FileScan am = null;
        try {
            am = new FileScan(heap_file_name, attrTypes, str_sizes,
                    (short)attrTypes.length, (short) attrTypes.length,
                    projections, null);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        // Get skyline elements
      //  BlockNestedLoopsSky blockNestedLoopsSky = null;
        try {
            System.out.println("Skyline computation");
            bnlskyline = new NestedLoopsSky(attrTypes, 1000, am, pref_list);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }



    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if( first_time ) {
            first_time = false;
            System.out.println("innn get next");
            compute_skyline();
        }
        System.out.println("out get next");
        return null;
    }



    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {
            try {
         //       outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }

    private void debug_printIndex(IndexScan iscan, int field_no){
        System.out.println( "----------Index scan----"  +field_no);
        Tuple t = null;
        int iout = 0;
        int ival = 100; // low key

        try {
            t = iscan.get_next();
        } catch (Exception e) {

            e.printStackTrace();
        }

        while (t != null) {
            try {
                iout = t.getIntFld(field_no);
                System.out.println( "iout = " + iout);
            } catch (Exception e) {

                e.printStackTrace();
            }

            ival = iout;

            try {
                t = iscan.get_next();
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
    }



}
