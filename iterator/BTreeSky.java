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
    ) throws IOException,NestedLoopException
   {
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
        ArrayList<ArrayList<Tuple>> hash_sets = new ArrayList<>();

        FldSpec[] projections = new FldSpec[attrTypes.length];
        RelSpec rel = new RelSpec(RelSpec.outer);

        for(int field_count = 0;  field_count < attrTypes.length; field_count++)
        {
            projections[field_count] = new FldSpec(rel, field_count+1);
        }

        for(int index_count = 0; index_count < index_files.length; index_count++)
        {
            java.lang.String index_file_name = ((BTreeFile) index_files[index_count]).getDbname();
            // start index scan on index of first preference attribute
            try
            {
                index_list[index_count] = new IndexScan(new IndexType(IndexType.B_Index), relation_name, index_file_name, attrTypes, str_sizes, 3, 3, projections, null, pref_list[index_count], false);
            } catch (Exception e) {
                e.printStackTrace();
            }

            hash_sets.add(new ArrayList<Tuple>());
            //debug_printIndex(index_list[index_count], pref_list[index_count]);
        }

        Tuple matched = null;
        Tuple temp = null;
        boolean flag = false;
        try
        {
            do
            {
                for(int index_count=0; index_count< index_files.length; index_count++)
                {
                    Tuple tt = (Tuple)index_list[index_count].get_next();
                    if(tt == null)
                    {
                        temp = tt;
                        flag = true;
                        break;
                    }


                    Tuple head = new Tuple(tt);
                    hash_sets.get(index_count).add(head);
                    temp = head;

                    boolean res = true;
                    for(int j=0; j < index_list.length; j++)
                    {
                       boolean search_result = false;
                       for(Tuple tuple : hash_sets.get(j))
                       {
                           if(TupleUtils.Equal(head, tuple, attrTypes, attrTypes.length))
                           {
                               search_result = true;
                           }
                       }
                       res = res & search_result;
                    }

                    // if we found a tuple in all dimension, break out as there is no need to check other tuples.
                    if(res)
                    {
                        System.out.println("Found a match in all dimensions");
                        matched = new Tuple(head);
                        flag = true;
                        break;
                    }
                }

                if(flag)
                {
                    // found a match in all dimension, simultaneous scan can be stopped now.
                    break;
                }
            }
            while(temp!=null);
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
            java.util.Iterator itr = hash_sets.get(index_count).iterator();
            while(itr.hasNext())
            {
                Tuple out = (Tuple)itr.next();
                if(TupleUtils.Equal(out, matched, attrTypes, attrTypes.length))
                {
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

        try
        {
            t = iscan.get_next();
        } catch (Exception e) {

            e.printStackTrace();
        }

        while (t != null)
        {
            try
            {
                iout = t.getIntFld(field_no);
                System.out.println( "iout = " + iout);
                t = iscan.get_next();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private void generate_skyline(List<Tuple> playersList)
    {
        RID rid;
        Heapfile f = null;
        java.lang.String heap_file_name = relation_name + "_tmp";
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

        FldSpec[] projections = new FldSpec[attrTypes.length];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int field_count = 0;  field_count < attrTypes.length; field_count++)
        {
            projections[field_count] = new FldSpec(rel, field_count+1);
        }

        FileScan am = null;
        try
        {
            am = new FileScan(heap_file_name, attrTypes, str_sizes,
                    (short)attrTypes.length, (short) attrTypes.length,
                    projections, null);
        }
        catch (Exception e)
        {
            System.err.println("" + e);
        }

        try
        {
            bnlskyline = new NestedLoopsSky(attrTypes, n_buf_pgs, am, pref_list);
        }
        catch (Exception e)
        {
            System.err.println("*** Error preparing for nested_loop_join - BtreeSky.java");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

}

