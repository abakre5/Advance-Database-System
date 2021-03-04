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



public class BTreeSky extends Iterator {
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

   public BTreeSky(AttrType[] attrs, int len_attr, short[] attr_size,
             int amt, Iterator left, java.lang.String
                     relation, int[] pref_list1, int[] pref_lengths_list,
             IndexFile[] index_file_list,
             int n_pages1
    ) throws IOException,NestedLoopException {
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
    1. start index scans on simultaneously on every index file
    2. Keep storing scanned elements in array list
    3. stop when tuple is found in all of the lists.
    4. Generate skyline by calling skyline algorithm
 */
    private void compute_skyline() throws Exception
    {
        // start index scan
        IndexScan[] index_list = new IndexScan[pref_list.length];
        Tuple[]  tuple_list = new Tuple[pref_list.length];
        ArrayList<ArrayList<Tuple>> hash_sets = new ArrayList<>();

        System.out.println(" Size of index_list " + index_list.length);
        System.out.println(" Size of tuple_list " + tuple_list.length);

        FldSpec[] projections = new FldSpec[3];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projections[0] = new FldSpec(rel, 1);
        projections[1] = new FldSpec(rel, 2);
        projections[2] = new FldSpec(rel, 3);


        for(int index_count = 0; index_count < index_files.length; index_count++) {

            java.lang.String index_file_name = ((BTreeFile) index_files[index_count]).getDbname();
            // start index scan on index of first preference attribute
            try
            {
                index_list[index_count] = new IndexScan(new IndexType(IndexType.B_Index), relation_name, index_file_name, attrTypes, str_sizes, 3, 3, projections, null, pref_list[index_count], false);
            } catch (Exception e) {
                //  status = FAIL;
                e.printStackTrace();
            }

            tuple_list[index_count] = null;
            hash_sets.add(new ArrayList<Tuple>());
            if(index_list[index_count] == null){
                System.out.println(" Failed to create index scan");
            }
            //debug_printIndex(index_list[index_count], pref_list[index_count]);
        }

        System.out.println(" Size of hash_sets " + hash_sets.size());

        Tuple matched = null;
        boolean flag = false;
        int count = 1;
        try
        {
            do
            {
                //System.out.println("-------Iteration count-------- " + count);
                for(int index_count=0; index_count< index_files.length; index_count++)
                {
                    Tuple tt = (Tuple)index_list[index_count].get_next();
                    if(tt == null)
                    {
                        tuple_list[index_count] = tt;
                        flag = true;
                        // System.out.println("found null " + i);
                        break;
                    }

                    Tuple head = new Tuple(tt);
                    hash_sets.get(index_count).add(head);
                    tuple_list[index_count] = head;

                    boolean res = true;
                    for(int j=0; j< index_list.length; j++) {
                       boolean search_result = false;
                       for(Tuple tuple : hash_sets.get(j))
                       {
                           if(TupleUtils.Equal(head, tuple, attrTypes, attrTypes.length)){
                               search_result = true;
                           }
                       }
                       res = res & search_result;
                    }

                    if(res){
                        System.out.println("Found a match");
                        matched = new Tuple(head);
                        flag = true;
                        break;
                    }
                }
                count++;

                if(flag){
                    break;
                }

            }while(tuple_list[0]!=null);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        System.out.println("Pruning done----------");

        List<Tuple> output = new ArrayList<Tuple>();
        output.add(matched);
        System.out.println("----------------Printing pruned list------------");
        System.out.println("matched " + matched.getIntFld(1) + " : " + matched.getIntFld(2) + " : " + matched.getIntFld(3));

        for(int index_count = 0; index_count < index_files.length; index_count++)
        {
            //System.out.println("size of hash_sets " + index_count +"\t"+ hash_sets.get(index_count).size());

            java.util.Iterator itr = hash_sets.get(index_count).iterator();
            while(itr.hasNext())
            {
                Tuple out = (Tuple)itr.next();
                if(TupleUtils.Equal(out, matched, attrTypes, attrTypes.length)){
                    break;
                }
                System.out.println(out.getIntFld(1) + " : " + out.getIntFld(2) + " : " + out.getIntFld(3));
                output.add(out);
            }
        }

        // This is needed to keep track of index files which we will be scanning from index_files array.
        System.out.println("size of tupleArrayList "  +output.size());


        generate_skyline(output);
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if( first_time ) {
            first_time = false;
            compute_skyline();
        }
        return bnlskyline.get_next();
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

    private void generate_skyline(List<Tuple> playersList)
    {

        RID rid;
        Heapfile f = null;
        java.lang.String heap_file_name = relation_name + "_rem";
        try
        {
            f = new Heapfile(heap_file_name);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        for (int i = 0; i <  playersList.size(); i++)
        {
            try
            {
                rid = f.insertRecord(playersList.get(i).returnTupleByteArray());
            }
            catch (Exception e)
            {
                System.err.println("*** error in Heapfile.insertRecord() ***");

                e.printStackTrace();
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
            bnlskyline = new NestedLoopsSky(attrTypes, 1000, am, pref_list);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

}

