package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import catalog.*;
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
    private int n_buf_pgs;
    private Iterator left_itr;
    private int[] pref_list;
    private IndexFile[] index_files;
    private int[] pref_lengths;
    private java.lang.String relation_name;
    private boolean first_time;
    private BlockNestedLoopsSky bnlskyline;
    private List<Tuple> skyline;

    /**
     * Constructor for BtreeSky
     * @param attrs -  array containing attribute types of the relation
     * @param len_attr - number of columns in the relation
     * @param attr_size - array of sizes of string attributes
     * @param left - an iterator for accessing the tuples
     * @param relation - name of the heap file
     * @param pref_list1 -  list of preference attributes
     * @param pref_lengths_list - number of preference attributes
     * @param index_file_list - array of index files on preference attributes
     * @param n_pages - amount of memory (in pages) available for sorting
     * @throws IOException
     */
   public BTreeSky(AttrType[] attrs, int len_attr, short[] attr_size,
            Iterator left, java.lang.String
                     relation, int[] pref_list1, int[] pref_lengths_list,
             IndexFile[] index_file_list,
             int n_pages
    ) throws IOException
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
        skyline = new ArrayList<>();
    }


    /**
     * Steps:
     * Initialize containers
     * 1. start index scans on simultaneously on every index file
     * 2. Keep storing rids of scanned tuples in each dimension in separate hash set
     *    and heap file(if memory exceeded)
     * 3. stop when tuple is found in all of the dimensions. All records coming after matched tuple
     *    can be safely ignored as it will be dominated by matched tuple.
     * 4. Scan through hashset and heap files till matched tuple and store the rids in one file
     * 5. Fetch actual tuples from disk and write them in separate file
     * 6. Generate skyline by calling BlockNested algorithm
     * @throws Exception
     */
    public void compute_skyline() throws Exception
    {
        // start index scan
        IndexFileScan[] index_list = new BTFileScan[pref_list.length];
        ArrayList<LinkedHashSet<RID>> hash_sets = new ArrayList<>();
        Heapfile[] heap_files = new Heapfile[pref_list.length];

        // tuple for storing rids in heap file
        AttrType[] attrs = new AttrType[2];
        attrs[0] = new AttrType (AttrType.attrInteger);
        attrs[1] = new AttrType (AttrType.attrInteger);

        //Initialize collections-  Create scans and heap files for each pref attribute
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
        }

        // Reference to RID object will take 4 and 8 bytes on 32 bit and 64 bit system.
        // Considering 8 bytes for RID object + 4 bytes for slotno(int)
        // + 8 bytes for PageID object + 4 bytes for pageno(int)
        int rid_size = 24;
        int rid_buffer_size = (int)Math.floor((GlobalConst.MINIBASE_PAGESIZE/rid_size) * n_buf_pgs);
        int rid_per_dim = rid_buffer_size / pref_list.length;

        //Get Tuple wrapper which will be used to insert rids in heap file.
        Tuple rid_tuple = getWrapperForRID();
        RID matched = new RID();
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

                    // If enough space in memory, then add to hashset else write the record on disk
                    if(hash_sets.get(index_count).size() < rid_per_dim)
                    {
                        hash_sets.get(index_count).add(rid);
                    }
                    else {
                        is_buffer_full = true;
                        heap_files[index_count].insertRecord(rid_tuple.getTupleByteArray());
                    }

                    //temp will be used to keep track if we reached end of scan.
                    temp = next_entry;

                    //Check rid of of current entry in all dimensions.
                    boolean bfound = true;
                    for(int dimension = 0; dimension < index_list.length; dimension++)
                    {
                        // As we have just inserted in this dimension, no need to check
                        if( dimension == index_count)
                        {
                            continue;
                        }

                       boolean search_result = hash_sets.get(dimension).contains(rid);
                       if(!search_result && is_buffer_full)
                       {
                           search_result |= checkInHeapFile(heap_files[dimension] , rid, attrs);
                       }
                       bfound = bfound & search_result;
                    }

                    // if we found a tuple in all dimension, break out as there is no need to check other tuples.
                    if(bfound)
                    {
                        matched.pageNo.pid = rid.pageNo.pid;
                        matched.slotNo = rid.slotNo;
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

        //Get rids of tuples from hash set and heap files of each dimension in a heap file.
        Heapfile hf  = getSkylineCanidates(hash_sets, attrs, is_buffer_full, matched, heap_files);

        try
        {
            for (int file_count = 0; file_count < heap_files.length; file_count++) {

                heap_files[file_count].deleteFile();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        generate_skyline(hf);
    }



    /**
     *  Creates a Heapfile object with given name
     * @param heapfileName - name of heap file
     * @return
     */
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



    /**
     * This function scans through hash sets and heap files in which rids are stored
     * for each dimension. it fetches record from passed rids and stores them in temporary file
     * temporary file is passed to blocknested for skyline computation.
     * @param hash_sets - Array of hash sets containing rids of records
     * @param attrs - array containing attribute types of the relation
     * @param is_buffer_full - boolean specifying if
     * @param matched - RID object which has been found in all dimensions
     * @param heap_files - array of heap files which contains rids.
     * @return it return a heap file.
     * @throws Exception
     */
    private Heapfile getSkylineCanidates(ArrayList<LinkedHashSet<RID>> hash_sets, AttrType[] attrs, boolean is_buffer_full, RID matched, Heapfile[] heap_files)  throws Exception
    {
        Heapfile hf = getHeapFileInstance("pruned_rids.in");
        Tuple rid_tuple = getWrapperForRID();

        //Insert matched record first
        rid_tuple.setIntFld(1, matched.pageNo.pid);
        rid_tuple.setIntFld(2, matched.slotNo);
        hf.insertRecord(rid_tuple.getTupleByteArray());

        boolean found_in_mem = false;
        for(int set_count = 0; set_count < hash_sets.size(); set_count++)
        {
            found_in_mem = false;
            java.util.Iterator itr = hash_sets.get(set_count).iterator();
            while(itr.hasNext())
            {
                RID out = (RID)itr.next();
                if(matched.pageNo.pid == out.pageNo.pid && matched.slotNo == out.slotNo)
                {
                    //No need to scan further as next tuples(whose rids are stored in hashset)
                    // will be dominated by matched tuple(with 'matched' rid)
                    found_in_mem = true;
                    break;
                }
                //System.out.println(out.pageNo + " : " + out.slotNo);
                rid_tuple.setIntFld(1, out.pageNo.pid);
                rid_tuple.setIntFld(2, out.slotNo);

                //Read the existing file and check if this rid has been inserted previously
                if(!checkInHeapFile(hf, out, attrs))
                {
                    hf.insertRecord(rid_tuple.getTupleByteArray());
                }
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
                        if (r != null)
                        {
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

                    if(!found_match && r!= null)
                    {
                        RID rt = new RID();
                        rt.pageNo.pid = rid_tuple.getIntFld(1);
                        rt.slotNo = rid_tuple.getIntFld(2);

                        //Read the existing file and check if this rid has been inserted previously
                        if(!checkInHeapFile(hf, rt, attrs))
                        {
                            hf.insertRecord(rid_tuple.getTupleByteArray());
                        }
                    }

                } while(rid_tuple!= null && !found_match);

                scan.closescan();
            }

            //As rids in this are inserted, we can safely clear this set
            hash_sets.get(set_count).clear();
        }

        return  hf;
    }

    /**
     * This method returns a tuple which can be used to store rids.
     * @return
     */
    private Tuple getWrapperForRID()
    {

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



    /**
     * This is utility function for checking if provided rid has been inserted in
     * given heap file as tuple.
     * @param hf - heap file to be checked
     * @param matched - RID object
     * @param attrs - attribute types
     * @return result of search as boolean
     */
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

        scan.closescan();

        return found_match;
    }



    /**
     *     This returns a skyline element. skyline is computed when this iterator is called
     *     for first time.
     * @return
     * @throws IOException
     * @throws JoinsException
     * @throws IndexException
     * @throws InvalidTupleSizeException
     * @throws InvalidTypeException
     * @throws PageNotReadException
     * @throws TupleUtilsException
     * @throws PredEvalException
     * @throws SortException
     * @throws LowMemException
     * @throws UnknowAttrType
     * @throws UnknownKeyTypeException
     * @throws Exception
     */
    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if( first_time ) {
            first_time = false;
            compute_skyline();
        }
        return bnlskyline.get_next();
    }

    /**
     * performs cleanup
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (!closeFlag) {
            try {
                bnlskyline.close();

            }catch (Exception e) {
                e.printStackTrace();
            }
            closeFlag = true;
        }
        skyline.clear();
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

    //RIDs of tuples remaining after pruning are given to this function in a file
    //It fetches tuple from main heap file and stores them in separate temp file
    //temp file is given to blocknested for skyline computation

    /**
     *
     * @param prunedElements -  heap file containing
     */
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
                        //Fetch tuple with rid from main heap file and store it in separate file
                        rid.pageNo.pid = rid_tuple.getIntFld(1);
                        rid.slotNo = rid_tuple.getIntFld(2);
                        Tuple tuple = (Tuple) record_file.getRecord(rid);
                        candidate_file.insertRecord(tuple.getTupleByteArray());
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

        }while(r!= null);

        try {
            scan.closescan();
            prunedElements.deleteFile();
        }catch (Exception e){
            e.printStackTrace();
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

        //Call block nested skyline iterator on remaining elements.
        try
        {
            bnlskyline = new BlockNestedLoopsSky(attrTypes, attrTypes.length, str_sizes, am, temp_file, pref_list, pref_list.length, n_buf_pgs);
            skyline.addAll(bnlskyline.getAllSkylineMembers());
            candidate_file.deleteFile();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void printSkyline(String materTableName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException, Catalogrelexists, Catalogmissparam, Catalognomem, RelCatalogException, Cataloghferror, Catalogdupattrs, Catalogioerror, FileAlreadyDeletedException {
        Heapfile file = null;
        attrInfo[] attrs = new attrInfo[len_in];
        if (checkToMaterialize(materTableName)) {
            int SIZE_OF_INT = 4;
            for (int i = 0; i < len_in; ++i) {
                attrs[i] = new attrInfo();
                attrs[i].attrType = new AttrType(attrTypes[i].attrType);
                attrs[i].attrName = "Col" + i;
                attrs[i].attrLen = (attrTypes[i].attrType == AttrType.attrInteger) ? SIZE_OF_INT : 32;
            }
            file = new Heapfile(materTableName);
        }
        int count = 0;
        for (Tuple tuple : skyline) {
            if (checkToMaterialize(materTableName)) {
                file.insertRecord(tuple.returnTupleByteArray());
            } else {
                tuple.print(attrTypes);
            }
            count++;
        }

        System.out.println("Skyline computation completed!");
        System.out.println("No of skyline members -> " + count);
        if (checkToMaterialize(materTableName)) {
            try {
                ExtendedSystemDefs.MINIBASE_RELCAT.createRel(materTableName, len_in, attrs);
            } catch (Exception e) {
                System.err.println("Error occurred while creating materialized view!");
                file.deleteFile();
            }
            System.out.println("Created materialize view! -> " + materTableName);
        }
        System.out.println("---------------------------------------------------------------------------------------------------------");
    }

    private boolean checkToMaterialize(String materTableName) {
        return (materTableName != null && materTableName.length() > 0);
    }
}

