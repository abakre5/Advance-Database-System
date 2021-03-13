package iterator;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
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
import index.IndexUtils;

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
    private ArrayList<Tuple> skyline = new ArrayList<Tuple>();
    private IndexFile[] index_files;
    private int[] pref_lengths;
    private java.lang.String relation_name;
    private boolean first_time;
    private java.util.Iterator skyline_iterator;
    private NestedLoopsSky bnlskyline;
    private ArrayList<Tuple> skyline_list = new ArrayList<Tuple>();
    private int buffer_threshold;
    private Heapfile disk = null;
    private int noOfDiskElements;
    private short noOfColumns;
    
    

   public BTreeSortedSky(AttrType[] attrs, int len_attr, short[] attr_size,
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
        buffer_threshold = -1;
        noOfDiskElements = 0;
        noOfColumns = (short) len_attr;
        
    }

/*
    Steps:
   
 */

    private void compute_skyline() throws Exception {
        BTFileScan index_scan = new BTFileScan();
        Heapfile hf = null;
        RID rid = new RID();
        KeyDataEntry KeyData = null;

        FldSpec[] projections = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projections[0] = new FldSpec(rel, 1);
        projections[1] = new FldSpec(rel, 2);
        projections[2] = new FldSpec(rel, 3);
        projections[3] = new FldSpec(rel, 4);
        projections[4] = new FldSpec(rel, 5);

        try {
            index_scan = ((BTreeFile)index_files[0]).new_scan(null, null);
        } catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("compute_skyline: index scan created");

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
            System.out.println("Key :" + KeyData.key + "Data: "+ KeyData.data);
            
            Tuple t = hf.getRecord(rid);
            
            //Please check why do we need to do this?
            Tuple current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
            current_tuple.setHdr((short)5, attrTypes, null);

            if(buffer_threshold == -1) {
                buffer_threshold = (int) Math.floor(Tuple.MINIBASE_PAGESIZE * n_buf_pgs / (int)current_tuple.size());
            }
            
            if(current_tuple!=null) {
                System.out.println("Size of tuple : "+ current_tuple.getFloFld(2));
            }
            
            if (is_skyline_candidate(current_tuple)) {
                //skyline_list.add(new Tuple(currentTuple));
                insert_skyline(current_tuple);
            }

            } while(KeyData!=null);

        } catch (Exception e){
            e.printStackTrace();
        }
        skyline.addAll(skyline_list);
        handleDiskMembers();
        System.out.println("================ Printing Skyline =======================\n");
        for(Tuple p: skyline) {
            System.out.println(p.getFloFld(1) + " : " + p.getFloFld(2) + " : " + p.getFloFld(3) + " : " + p.getFloFld(4) + " : "+p.getFloFld(5));
        }
       
        System.out.println("================ End Skyline =======================\n");
    }

    

    private boolean is_skyline_candidate(Tuple curr_tuple) throws IOException, TupleUtilsException {
        for (Tuple tupleInSkyline : skyline_list) {
            if (TupleUtils.Dominates(tupleInSkyline, attrTypes, curr_tuple, attrTypes, (short)attrTypes.length, str_sizes, pref_list, pref_list.length)) {
                return false;
            }
        }
        System.out.println("Return True");
        return true;
    }

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
        }
    }

    private Heapfile getHeapFileInstance() throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        String relationName = "sortFirstDisk.in";
        return new Heapfile(relationName);
    }
    private FileScan getFileScan() throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;
        String relationName = "sortFirstDisk.in";

        FldSpec[] Pprojection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, str_sizes,
                noOfColumns, noOfColumns, Pprojection, null);
        return scan;
    }


    private void handleDiskMembers() throws Exception {
        System.out.println("handleDiskMembers called..");
        if (disk != null && noOfDiskElements > 0) {
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
            skyline.addAll(skyline_list);
            handleDiskMembers();
        }
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
        if( !skyline.isEmpty() ) {
            Tuple nextTuple = skyline.get(0);
            skyline.remove(0);
           // System.out.println("out get next2");
            return nextTuple;
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
