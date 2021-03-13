package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import global.*;
import heap.*;
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
            Iterator left, java.lang.String
                     relation, int[] pref_list1, int[] pref_lengths_list,
             IndexFile[] index_file_list,
             int n_pages
    ) throws IOException,NestedLoopException
   {
        attrTypes = attrs;
        len_in = len_attr;
        str_sizes = attr_size;
        left_itr = left;
        relation_name = relation;
        pref_list = pref_list1;
        pref_lengths = pref_lengths_list;
        index_files = index_file_list;
        n_buf_pgs = n_pages;
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
        Heapfile[] heap_files = new Heapfile[pref_list.length];
        // tuple for storing rids heapfile
        AttrType[] attrs = new AttrType[2];
        attrs[0] = new AttrType (AttrType.attrInteger);
        attrs[1] = new AttrType (AttrType.attrInteger);

        for(int index_count = 0; index_count < index_files.length; index_count++)
        {
            try
            {
                index_list[index_count] = (BTFileScan) IndexUtils.BTree_scan(null, index_files[index_count]);
                heap_files[index_count] = new Heapfile(null);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            hash_sets.add(new LinkedHashSet<RID>());
            //debug_printIndex(index_list[index_count], pref_list[index_count]);
        }

        RID sample_rid = new RID();
        int rid_buffer_size = (int)Math.floor(GlobalConst.MINIBASE_PAGESIZE * n_buf_pgs /  22);
        int rid_per_dim = rid_buffer_size / pref_list.length;
        //ObjectSizeFetcher.getObjectSize(sample_rid) );
        System.out.println("Allowed rid per dim  " + rid_per_dim);

        Tuple rid_tuple = getWrapperForRID();
        RID matched = null;
        KeyDataEntry temp = null;
        boolean bstop_search = false;
        boolean is_buffer_full = false;

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

                    RID rid = ((LeafData) next_entry.data).getData();
                    rid_tuple.setIntFld(1, rid.pageNo.pid);
                    rid_tuple.setIntFld(2, rid.slotNo);

                    if(hash_sets.get(index_count).size() < rid_per_dim)
                    {
                        //System.out.println("Inserting in mem");
                        hash_sets.get(index_count).add(rid);
                    }
                    else {
                       // System.out.println("Inserting on disk");
                        is_buffer_full = true;
                        heap_files[index_count].insertRecord(rid_tuple.getTupleByteArray());
                    }

                    temp = next_entry;

                    boolean bfound = true;
                    for(int j=0; j < index_list.length; j++)
                    {
                        // As we have just inserted in this dimension, no need to check
                        if( j == index_count)
                        {
                            continue;
                        }

                       boolean search_result = hash_sets.get(j).contains(rid);
                       if(!search_result && is_buffer_full)
                       {
                           search_result |= checkInHeapFile(heap_files[j] , rid, attrs);
                       }
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
            }
            while(temp!=null && !bstop_search);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        Heapfile hf  = getSkylineCanidates(hash_sets, attrs, is_buffer_full, matched, heap_files);
        generate_skyline(hf);
    }

    private Heapfile getHeapFileInstance(java.lang.String heapfileName)
    {
        Heapfile hf = null;
        try
        {
            hf = new Heapfile(heapfileName);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }

        return hf;
    }

    private Heapfile getSkylineCanidates(ArrayList<LinkedHashSet<RID>> hash_sets, AttrType[] attrs, boolean is_buffer_full, RID matched, Heapfile[] heap_files)  throws Exception
    {
        Heapfile hf = getHeapFileInstance("pruned_rids.in");
        Tuple rid_tuple = getWrapperForRID();

        rid_tuple.setIntFld(1, matched.pageNo.pid);
        rid_tuple.setIntFld(2, matched.slotNo);
        hf.insertRecord(rid_tuple.getTupleByteArray());

        boolean found_in_mem = false;
        for(int set_count = 0; set_count < hash_sets.size(); set_count++)
        {
            //System.out.println("Set size : " +  hash_sets.get(set_count).size());
            found_in_mem = false;
            java.util.Iterator itr = hash_sets.get(set_count).iterator();
            while(itr.hasNext())
            {
                RID out = (RID)itr.next();
                if(matched.equals(out))
                {
                    found_in_mem = true;
                    break;
                }
                //System.out.println(out.pageNo + " : " + out.slotNo);
                rid_tuple.setIntFld(1, out.pageNo.pid);
                rid_tuple.setIntFld(2, out.slotNo);
                hf.insertRecord(rid_tuple.getTupleByteArray());
            }

            // if we found the record in memory, no need to look into tuples
            // in temp files as they can be pruned.
            if(!found_in_mem && is_buffer_full)
            {
                Scan scan  = heap_files[set_count].openScan();
                RID scan_rid = new RID();
                boolean found_match = false;

                do {
                    Tuple r = null;
                    try
                    {
                        r = (Tuple) scan.getNext(scan_rid);
                        if (r != null) {
                            rid_tuple = new Tuple(r.getTupleByteArray(), r.getOffset(), r.getLength());
                            rid_tuple.setHdr((short) 2, attrs, null);

                            if(rid_tuple.getIntFld(1) == matched.pageNo.pid &&
                                    rid_tuple.getIntFld(2) == matched.slotNo )
                            {
                                found_match =true;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }

                    if(!found_match && rid_tuple != null)
                    {
                        hf.insertRecord(rid_tuple.getTupleByteArray());
                    }

                } while(rid_tuple!= null && !found_match);
            }
            hash_sets.get(set_count).clear();
        }

        return  hf;
    }

    private Tuple getWrapperForRID(){

        // tuple for storing rids heapfile
        AttrType[] Ptypes = new AttrType[2];
        Ptypes[0] = new AttrType (AttrType.attrInteger);
        Ptypes[1] = new AttrType (AttrType.attrInteger);

        Tuple rid_tuple = new Tuple();
        try {
            rid_tuple.setHdr((short) 2,Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = rid_tuple.size();
        rid_tuple = new Tuple(size);
        try {
            rid_tuple.setHdr((short) 2, Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        return rid_tuple;

    }

    private boolean checkInHeapFile(Heapfile hf, RID matched, AttrType[] attrs)
    {
        Tuple rid_tuple = null;
        Scan scan = null;

        try {
            scan= hf.openScan();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        RID scan_rid = new RID();
        boolean found_match = false;
        Tuple r = null;

        do{

            try
            {
                r = (Tuple)scan.getNext(scan_rid);
                if(r != null)
                {
                    rid_tuple = new Tuple(r.getTupleByteArray(), r.getOffset(), r.getLength());
                    rid_tuple.setHdr((short) 2, attrs, null);

                    if( rid_tuple != null && rid_tuple.getIntFld(1) == matched.pageNo.pid
                            && rid_tuple.getIntFld(2) == matched.slotNo )
                    {
                        found_match =true;
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

        }while(r!= null && !found_match);

        return found_match;
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

    private void generate_skyline(Heapfile prunedElements)
    {
        Heapfile record_file = getHeapFileInstance(relation_name);
        java.lang.String temp_file = relation_name + "_tmp";
        Heapfile candidate_file = getHeapFileInstance(temp_file);
        Scan scan = null;

        AttrType[] attrs = new AttrType[2];
        attrs[0] = new AttrType (AttrType.attrInteger);
        attrs[1] = new AttrType (AttrType.attrInteger);

        try {
            scan = prunedElements.openScan();
        }catch (Exception e){
            e.printStackTrace();
        }

        Tuple rid_tuple = null;
        Tuple r = null;
        RID rid = new  RID();
        RID scan_rid = new RID();
        do{
            try
            {
                r = (Tuple)scan.getNext(scan_rid);
                if(r != null)
                {
                    rid_tuple = new Tuple(r.getTupleByteArray(), r.getOffset(), r.getLength());
                    rid_tuple.setHdr((short) 2, attrs, null);

                    if( rid_tuple != null)
                    {
                        rid.pageNo.pid = rid_tuple.getIntFld(1);
                        rid.slotNo = rid_tuple.getIntFld(2);
                        Tuple tuple = (Tuple) record_file.getRecord(rid);
                        candidate_file.insertRecord(tuple.getTupleByteArray());
;
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

        }while(r!= null);

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

