package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import global.*;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.IndexUtils;


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
        IndexFileScan[] index_list = new BTFileScan[pref_list.length];
        ArrayList<LinkedHashSet<RID>> hash_sets = new ArrayList<>();

        FldSpec[] projections = new FldSpec[attrTypes.length];
        RelSpec rel = new RelSpec(RelSpec.outer);

        for(int field_count = 0;  field_count < attrTypes.length; field_count++)
        {
            projections[field_count] = new FldSpec(rel, field_count+1);
        }

        for(int index_count = 0; index_count < index_files.length; index_count++)
        {
            try
            {
                index_list[index_count] = (BTFileScan) IndexUtils.BTree_scan(null, index_files[index_count]);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            hash_sets.add(new LinkedHashSet<RID>());
            //debug_printIndex(index_list[index_count], pref_list[index_count]);
        }

        RID matched = null;
        KeyDataEntry temp = null;
        boolean bstop_search = false;
        try
        {

            do
            {
                for(int index_count=0; index_count< index_files.length; index_count++)
                {
                    KeyDataEntry next_entry = index_list[index_count].get_next();
                    if(next_entry == null)
                    {
                        temp = null;
                        bstop_search = true;
                        break;
                    }

                    RID  rid = ((LeafData) next_entry.data).getData();
                    hash_sets.get(index_count).add(rid);
                    temp = next_entry;

                    boolean bfound = true;
                    for(int j=0; j < index_list.length; j++)
                    {
                       boolean search_result = hash_sets.get(j).contains(rid);
                       bfound = bfound & search_result;
                    }

                    // if we found a tuple in all dimension, break out as there is no need to check other tuples.
                    if(bfound)
                    {
                        System.out.println("Found a match in all dimensions");
                        matched = rid;
                        bstop_search = true;
                        break;
                    }
                }

                if(bstop_search)
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

        HashSet<RID> output = new HashSet<RID>();
        output.add(matched);

       // System.out.println("----------------Printing pruned list------------");

        for(int set_count = 0; set_count < hash_sets.size(); set_count++)
        {
            //System.out.println("Set size : " +  hash_sets.get(set_count).size());
            java.util.Iterator itr = hash_sets.get(set_count).iterator();
            while(itr.hasNext())
            {
                RID out = (RID)itr.next();
                if(matched.equals(out))
                {
                    break;
                }

                //System.out.println(out.pageNo + " : " + out.slotNo);
                output.add(out);
            }
            hash_sets.get(set_count).clear();
        }

        System.out.println("outputSet size : " +  output.size());
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

    private void generate_skyline(HashSet<RID> prunedElements)
    {
        Heapfile record_file = null;
        Heapfile candidate_file = null;
        java.lang.String temp_file = relation_name + "_tmp";

        try
        {
            record_file = new Heapfile(relation_name);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        try
        {
            candidate_file = new Heapfile(temp_file);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        for (RID rid : prunedElements)
        {
            try
            {
                Tuple tuple = (Tuple)record_file.getRecord(rid);
                candidate_file.insertRecord(tuple.getTupleByteArray());
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
            am = new FileScan(temp_file, attrTypes, str_sizes,
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

