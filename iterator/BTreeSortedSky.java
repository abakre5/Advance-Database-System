package iterator;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.IndexFile;
import btree.KeyDataEntry;
import btree.LeafData;
import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.*;



/**
 * @author : Shubham Hemant Chakrawar
 *
 */
public class BTreeSortedSky extends Iterator {
    private AttrType[] attrTypes;
    private int len_in;
    private short[] str_sizes;
    private int n_buf_pgs;
    private int[] pref_list;
    private ArrayList<Tuple> skyline = new ArrayList<Tuple>();
    private IndexFile[] index_files;
    private int[] pref_lengths;
    private java.lang.String relation_name;
    private boolean first_time;
    private ArrayList<Tuple> skyline_list = new ArrayList<Tuple>();
    private int buffer_threshold;
    private Heapfile disk = null, outHeapfile = null;
    private int noOfDiskElements;
    private short noOfColumns;
    private boolean isOutFilePresent;
    private FileScan get_next_scan = null;
    private boolean first_next = true;
    
   public BTreeSortedSky(AttrType[] attrs, int len_attr, short[] attr_size,
             int amt, Iterator left, java.lang.String
                     relation, int[] pref_list1, int[] pref_lengths_list,
             IndexFile[] index_file_list,
             int n_pages1
    ) throws IOException,NestedLoopException {
        attrTypes = attrs;
        len_in = len_attr;
        str_sizes = attr_size;
        relation_name = relation;
        pref_list = pref_list1;
        pref_lengths = pref_lengths_list;
        index_files = index_file_list;
        n_buf_pgs = n_pages1;
        first_time = true;
        buffer_threshold = -1;
        noOfDiskElements = 0;
        noOfColumns = (short) len_attr;
        
    }

 /*   Steps:
    Initialize containers
    1. start index scan on the index file.
    2. Fetch record for the retrived RID.
    3. Add the first record to skyline list, as it is always going to be the skyline.
    4. Compare record belonging to i+1 index entry with "i" th. If i dominates i+1, scrap i+1 tuple.
        Otherwise add it to the skyline_list. If skyline_list has crossed buffer threshold size, then
        add this incoming tuple to the btreesortedDisk.in file and flush the skyline_list to output file
        as skyline_list is always going to contain confirmed skyline points.
        This file will store all the probable skyline candidates which are yet to be compared with each other.
    5. Process this disk file to compare these tuples with each other. Discard the ones which are dominated by the 
        other tuples. 
    6. Everytime skyline_list gets full, flush it to the output file of confirmed skyline points.
 */

    private void compute_skyline() throws Exception {

        BTFileScan index_scan = new BTFileScan();
        Heapfile hf = null;
        RID rid = new RID();
        KeyDataEntry KeyData = null;

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }

        // start index scan
        try {
            index_scan = ((BTreeFile)index_files[0]).new_scan(null, null);
        } catch(Exception e) {
            e.printStackTrace();
        }

        //open input heapfile.
        try {
            System.out.println("Relation: "+ relation_name);
            hf = new Heapfile(relation_name);
          
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            do {
            KeyData = index_scan.get_next();
            if (KeyData!=null) {
                rid = ((LeafData) KeyData.data).getData();
            } else {
                break;
            }
            
            //Fetch the record from heapfile using rid obtained from the index.
            Tuple t = hf.getRecord(rid);
            
            Tuple current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
            current_tuple.setHdr((short)len_in, attrTypes, null);

            //Buffer threshold = Number of tuples that can be accomodated in memory window called skyline_list.
            //This number is arrived at using n_pages attributes and minibase page size.

            if(buffer_threshold == -1) {
                buffer_threshold = (int) Math.floor(Tuple.MINIBASE_PAGESIZE * n_buf_pgs / (int)current_tuple.size());
                System.out.println("Threshold of Buffer: " + buffer_threshold);
                System.out.println("Minibase page size : " + Tuple.MINIBASE_PAGESIZE);

            }

            //This file contains output skyline elements.
            outHeapfile = getSkylineFileInstance();

            if (is_skyline_candidate(current_tuple)) {
                insert_skyline(current_tuple);
            }

            } while(KeyData!=null);

        } catch (Exception e){
            e.printStackTrace();
        }

        //flush the window to output heap file.
        if(!skyline_list.isEmpty()) {
            addtoSkyline(skyline_list);
        }
        handleDiskMembers(); 
    }

    

    private boolean is_skyline_candidate(Tuple curr_tuple) throws Exception,IOException, TupleUtilsException, FileScanException, InvalidRelation {
        for (Tuple tupleInSkyline : skyline_list) {
            if (TupleUtils.Dominates(tupleInSkyline, attrTypes, curr_tuple, attrTypes, (short)attrTypes.length, str_sizes, pref_list, pref_list.length)) {
                return false;
            }
        }

        if (isOutFilePresent) {
            FileScan scan = getSkylineFileScan();
            Tuple tupleInSkyline = scan.get_next();
            while(tupleInSkyline!=null) {

                if (TupleUtils.Dominates(tupleInSkyline, attrTypes, curr_tuple, attrTypes, (short)attrTypes.length, str_sizes, pref_list, pref_list.length)) {
                    return false;
                }
                tupleInSkyline = scan.get_next();
            }
            scan.close();
        }
        return true;
    }

    //If skyline_list window is full(>buffer_threshold), it means more records can not be accomodated.
    //Pass the incoming records to the diskfile.
    private void insert_skyline(Tuple curr_tuple) throws IOException,InvalidTupleSizeException, 
                                                HFBufMgrException, HFException, SpaceNotAvailableException,InvalidTypeException,
                                                InvalidSlotNumberException,HFDiskMgrException { 
        if(skyline_list.size() < buffer_threshold) {
            skyline_list.add(curr_tuple);
        } else {
            if(disk == null) {
                disk = getHeapFileInstance();
            }
            disk.insertRecord(curr_tuple.getTupleByteArray());
            noOfDiskElements++;
            System.out.println("Number of disk of elements "+ noOfDiskElements);  

        }

    }

    private Heapfile getHeapFileInstance() throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        String relationName = "btreesortedDisk.in";
        return new Heapfile(relationName);
    }
    private FileScan getFileScan() throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;
        String relationName = "btreesortedDisk.in";

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, str_sizes,
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }

    private Heapfile getSkylineFileInstance() throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        String relationName = "Skyline.out";
        return new Heapfile(relationName);
    }
    private FileScan getSkylineFileScan() throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;
        String relationName = "Skyline.out";

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, str_sizes,
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }

    /*
        - Function to handle probable skyline candidate tuples stored in disk file (They were redirected here when window was full).
        - This tuples were yet to be compared with each other.
        - Tuples which are examined in the process are deleted from the disk. So that they aren't compared in the next pass.
        - This process operates in a recursive manner unless and untill all the elements from the disk are vetted.

    */


    private void handleDiskMembers() throws Exception {
        if (disk != null && noOfDiskElements > 0) {
            System.out.println("handleDiskMembers called..");
            skyline_list.clear();
            HashSet<RID> vettedTuples = new HashSet<>();
            FileScan diskOuterScan = getFileScan();
            TupleRIDPair tupleRIDPairOuter = diskOuterScan.get_next1();
            while (tupleRIDPairOuter != null) {
                Tuple tupleOuter = tupleRIDPairOuter.getTuple();
                RID ridOuter = tupleRIDPairOuter.getRID();
                Tuple diskTupleToCompare = new Tuple(tupleOuter);

                if (skyline_list.size() < buffer_threshold) {
                    if (is_skyline_candidate(diskTupleToCompare)) {
                        skyline_list.add(diskTupleToCompare);
                    }
                    vettedTuples.add(ridOuter);
                } else {
                    break;
                }
                tupleRIDPairOuter = diskOuterScan.get_next1();
            }
            for (RID ridToDelete : vettedTuples) {
                disk.deleteRecord(ridToDelete);
                noOfDiskElements--;
            }
            diskOuterScan.close();
            
            //skyline.addAll(skyline_list);
            if (!skyline_list.isEmpty()) {
                addtoSkyline(skyline_list);
            }
            handleDiskMembers();
        }
    }

  
    //Adds skyline list to the output file.
    private void addtoSkyline(ArrayList<Tuple> skylines) throws Exception {
        isOutFilePresent = true;
        for (Tuple tupleInSkyline : skylines) {
            outHeapfile.insertRecord(tupleInSkyline.getTupleByteArray());
        }
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if( first_time ) {
            first_time = false;
            compute_skyline();
        }

        //This will fetch the skyline records from the output file and keep returning it to the test driver.
        if(isOutFilePresent && first_time == false) {
            if (first_next) {
                get_next_scan = getSkylineFileScan();
                first_next = false;
            }
            Tuple tuple = get_next_scan.get_next();
         
            return tuple;
        }
        
        return null;
    }



    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {
            try {
         //       outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "BTreeSortedSky.java: error in closing iterator.");
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
