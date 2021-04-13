package hash;

import java.io.*;
import java.security.KeyException;
import java.util.*;
import hash.FloatKey;
import iterator.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.TupleRIDPair;

public class HashFile extends IndexFile implements GlobalConst {

    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private int indexField;
    private String relationName;
    private Scan relationScan = null;
    private int keyType;
    AttrType[] attrType;
    int numAttribs;
    boolean split = false;
    boolean split_exists = false;
    int split_position = 0;
    double threshold = 0.75;
    int num_buckets;
    int n = 0;
    double current_util = 0.0;
    int total_records = 0;
    boolean is_crossed = false;
    Map<Integer,String> map = new HashMap<Integer,String>();
    Heapfile headerFile = null;
    int crossed =0;
    Heapfile datafile = null;
    String hashIndexName;



  
    //Constructor
    public HashFile(String relationName,String hashFileName, int indexField, int keyType, FileScan scan, int num_records, Heapfile dbfile) throws IOException, HFException, HFDiskMgrException, HFBufMgrException,
                 InvalidTupleSizeException,InvalidSlotNumberException {
        
        
        System.out.println("Hello");
        hashIndexName = hashFileName;
        datafile = dbfile;
        headerFile = new Heapfile(hashIndexName);
        System.out.println("Num of records = " + num_records);
        
        Tuple t = getWrapperForRID();
        int t_size = t.size();
        numAttribs = 3;
        //Tuples per page
        n =  (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / ((t_size) - 4) ;
        System.out.println(n);
        double buckets = Math.ceil(num_records / (n * threshold));
        num_buckets = (int) buckets;
        System.out.println("Number of buckets : "+ num_buckets);
        
        
        // This tuple corresponds to entry in a heap file.
        attrType = new AttrType[numAttribs];
        //short[] attrSize = new short[numAttribs];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        try {
            t.setHdr((short)numAttribs, attrType, null);

        } catch (Exception e) {
            e.printStackTrace();
        }

        //Tuple for header
        Tuple h = wrapperForHeader();
        int h_size = h.size();
        int numHeaderAttribs = 2;
        // This tuple corresponds to entry in a heap file.
        short[] attrSize = new short[1];
        
        attrSize[0] = REC_LEN1;
        attrType = new AttrType[2];
        //short[] attrSize = new short[numAttribs];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrString);

        try {
            h.setHdr((short)numHeaderAttribs, attrType, attrSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        //Create buckets

        for(int i =0; i<num_buckets; i++) {
            String FileName = "hash_buckets_"+i;
            Heapfile hf = new Heapfile(FileName);
            map.put(i, FileName);
        }
        
        //Populate headerFile entries.
        for (Map.Entry<Integer, String> set : map.entrySet()) {
		    System.out.println(set.getKey() + " = " + set.getValue());

            try {
                h.setIntFld(1, (int)set.getKey());
                h.setStrFld(2, set.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                headerFile.insertRecord(h.returnTupleByteArray());
            } catch (Exception e) {
                e.printStackTrace();
            }

		}


        //Let's iterate through data file and try inserting data in individual buckets.
        TupleRIDPair dataPair;
        Tuple data = null;
        RID rid = null;
        int ii = 0;
        try {
            dataPair = scan.get_next1();
            data = dataPair.getTuple();
            rid = dataPair.getRID();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        AttrType[] attrTypes = new AttrType[5];
        //short[] attrSize = new short[numAttribs];
        for (int i = 0; i < 2; ++i) {
            attrTypes[i] = new AttrType(AttrType.attrReal);
        }
        try {
            data.setHdr((short) 2, attrTypes, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Float value = null;
        int bucket=-1;
        String bucket_name = "";
        while(data !=null) {

            try {
                //System.out.println("Data: "+ value);
                value = data.getFloFld(1);
                bucket = get_hash(value);
                
                bucket_name = map.get(bucket);
                //System.out.println("Bucket Name is --> "+ bucket_name);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {

                Heapfile bucketFile = new Heapfile(bucket_name);
                Tuple bucketEntry = new Tuple();
                attrSize = new short[2];
                attrSize[0] = REC_LEN1;
                attrSize[1] = REC_LEN1;
                attrType = new AttrType[3];
                //short[] attrSize = new short[numAttribs];
                attrType[0] = new AttrType(AttrType.attrReal);
                attrType[1] = new AttrType(AttrType.attrInteger);
                attrType[2] = new AttrType(AttrType.attrInteger);
                bucketEntry.setHdr((short)3,attrType, attrSize);

                bucketEntry.setFloFld(1, value);
                bucketEntry.setIntFld(2, rid.pageNo.pid);
                bucketEntry.setIntFld(3, rid.slotNo);
                // if(bucket ==0) {
                //     System.out.println("Insert debug" +value + " "+ rid.pageNo.pid + " "+rid.slotNo);
                // }

                rid = bucketFile.insertRecord(bucketEntry.getTupleByteArray());
                ii++;
                dataPair = scan.get_next1();
                if (dataPair!=null) {
                    data = dataPair.getTuple();
                    rid = dataPair.getRID();
                } else {
                    data = null;
                    System.out.println(ii);
                }
             
                

            } catch (Exception e) {
                e.printStackTrace();
            }
           
        }
        
        System.out.println("Printing current utilization:-> "); 
        System.out.println(ii);
        System.out.println(num_buckets);
        System.out.println(n);

        double a = (num_buckets*n);
        a = ii/a;
        total_records = ii;
        current_util = a;
        System.out.println(current_util);

        System.out.println("Printing hash bucket 0");
        // FileScan s =  null;
        // Tuple tuple = null;
        // try {
        //     s = getFileScan("hash_buckets_0");
        //     tuple = s.get_next();
        // } catch (Exception e){
        //     e.printStackTrace();
        // }

        // while(tuple!=null) {
        //     System.out.println("tuple");
        //     try{
        //     System.out.println(tuple.getFloFld(1)+" "+ tuple.getIntFld(2)+ " "+tuple.getIntFld(3));
        //     tuple = s.get_next();
        //     } catch (Exception e) {
        //         e.printStackTrace();
        //     }
            
        // }

       
      


	}
        
    //Constructor
    public HashFile(String hashFileName) throws IOException,HFException,HFBufMgrException,HFBufMgrException,HFDiskMgrException{
        hashIndexName = hashFileName;
        headerFile = new Heapfile(hashIndexName);
    }

    private FileScan getFileScan(String fname) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;

        FldSpec[] Pprojection = new FldSpec[3];
        for (int i = 1; i <= 3; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        attrType = new AttrType[numAttribs];
        //short[] attrSize = new short[numAttribs];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN1;
       
        scan = new FileScan(fname, attrType, attrSize,(short)3,3, Pprojection, null);
        return scan;
    }

      /**
     * This method returns a tuple which can be used to store rids.
     * @return
     */
    public Tuple getWrapperForRID()
    {

        // tuple for storing key, rids heapfile
        AttrType[] Ptypes = new AttrType[3];

        Ptypes[0] = new AttrType (AttrType.attrReal);
        Ptypes[1] = new AttrType (AttrType.attrInteger);
        Ptypes[2] = new AttrType (AttrType.attrInteger);

        Tuple rid_tuple = new Tuple();
        try {
            rid_tuple.setHdr((short) 3,Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = rid_tuple.size();
        rid_tuple = new Tuple(size);
        try {
            rid_tuple.setHdr((short) 3, Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        return rid_tuple;

    }

    public Tuple wrapperForHeader() {
        // tuple for storing hash value -> bucket map
        short[] attrSize = new short[1];
        
        attrSize[0] = REC_LEN1;
        AttrType[] Ptypes = new AttrType[2];

        Ptypes[0] = new AttrType (AttrType.attrInteger);
        Ptypes[1] = new AttrType (AttrType.attrString);
       

        Tuple header_tuple = new Tuple();
        try {
            header_tuple.setHdr((short) 2,Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = header_tuple.size();
        header_tuple = new Tuple(size);
        try {
            header_tuple.setHdr((short) 2, Ptypes, attrSize);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        return header_tuple;

    }
    //PlaceHolders for delete/insert.

    public void insert(hash.KeyClass key, RID rid) throws IOException, FieldNumberOutOfBoundException, HFException, HFDiskMgrException, HFBufMgrException,
    InvalidBufferException,InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, FileAlreadyDeletedException {

        FloatKey floatKey = (FloatKey)key;
        Float keyValue = floatKey.getKey();
      
        Tuple indexEntry = getWrapperForRID();
        int indexEntry_size = indexEntry.size();
        numAttribs = 3;
        
        
        // This tuple corresponds to entry in a heap file.
        attrType = new AttrType[numAttribs];
        //short[] attrSize = new short[numAttribs];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN1;


        try {
            indexEntry.setHdr((short)numAttribs, attrType, attrSize);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            indexEntry.setFloFld(1, keyValue);
            indexEntry.setIntFld(2, rid.pageNo.pid);
            indexEntry.setIntFld(3, rid.slotNo);
        } catch (Exception e) {
            e.printStackTrace();
        }

        int index_num = get_hash(keyValue);
        //System.out.println("Index Num: "+index_num);
        String bucket_file = map.get(index_num);
        Heapfile bucketFile = new Heapfile(bucket_file);
        
        try {
            bucketFile.insertRecord(indexEntry.getTupleByteArray());
            total_records++;
        } catch(Exception e) {
            e.printStackTrace();
        }

        double a = (num_buckets*n);
        a = total_records/a;        
        current_util = a;
        if(current_util >= threshold) {

            System.out.println("Target utilization has been crossed: "+current_util );
            crossed++;
            split = true;
            split_exists = true;
            //split_position++;
            num_buckets++;
            String FileName = "hash_buckets_"+num_buckets;
            String FileName_orig_dash = "hash_buckets_"+split_position+"_1";
            String orig_File = map.get(split_position);
            Heapfile hf = new Heapfile(FileName);
            Heapfile hf1 = new Heapfile(FileName_orig_dash);
            Heapfile hf2 = new Heapfile(orig_File);
            map.put(num_buckets, FileName);

            Tuple h = wrapperForHeader();
            Tuple h1 = wrapperForHeader();
            
            //Add the entry for new bucket into the header file.
            try {
                h.setIntFld(1, num_buckets);
                h.setStrFld(2, FileName);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                headerFile.insertRecord(h.returnTupleByteArray());
                //System.out.println("Header file updated");
            } catch (Exception e) {
                e.printStackTrace();
            }
            Scan bucket_scan = null;
            String fname = map.get(split_position);
            System.out.println("Trying to open : "+fname);
            //System.out.println(fname);
            //bucketFile = new Heapfile(fname);

            FileScan s =  null;
            Tuple tuple = null;
            try {
                s = getFileScan(fname);
                tuple = s.get_next();
            } catch (Exception e){
                e.printStackTrace();
            }

            while(tuple!=null) {
                //System.out.println("tuple");
                try{
                    if (tuple!=null) {
                        //System.out.println("Try");
                        Float value = tuple.getFloFld(1);
                        int hash2_value = get_hash(value);
    
                        if(hash2_value == split_position) {
                            System.out.println("Hashed to same file");
                            hf1.insertRecord(tuple.getTupleByteArray());
    
                        } else {
                            System.out.println("Hashed to new file");
                            hf.insertRecord(tuple.getTupleByteArray());
                        }
                    } else {
                        break;
                    }
                
                    tuple = s.get_next();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        

        //Delete old file
        hf2.deleteFile();
         
        map.remove(split_position);
        map.put(split_position, FileName_orig_dash);

        split = false;
        split_position++;
        System.out.println("Crossed value : "+crossed);
    }   


    //Printing Index
}




    //Debug function to print the index.
    public void printindex() throws IOException {
        int total_count = 0;
        for (Map.Entry<Integer, String> set : map.entrySet()) {
		    System.out.println(set.getKey() + " = " + set.getValue());
            try {
                Heapfile h = new Heapfile(set.getValue());
                //Heapfile data = new Heapfile("nc_2_7000_single.txt");
                total_count = total_count + h.getRecCnt();

                FileScan scan = getFileScan(set.getValue());
                Tuple tuple = scan.get_next();
                RID rid = new RID();

                while(tuple!=null) {
                    try {
                        //System.out.println("Key: "+ tuple.getFloFld(1) + ", "+ tuple.getIntFld(2)+ ":"+tuple.getIntFld(3));
                        rid.pageNo.pid = tuple.getIntFld(2);
                        rid.slotNo = tuple.getIntFld(3);
                        Tuple t = datafile.getRecord(rid);
                        Tuple current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
                        attrType = new AttrType[2];
                        //short[] attrSize = new short[numAttribs];
                        for (int i = 0; i < 2; ++i) {
                            attrType[i] = new AttrType(AttrType.attrReal);
                        }
                        current_tuple.setHdr((short)2, attrType, null);
                        System.out.println("Data Tuple "+current_tuple.getFloFld(1) + " "+current_tuple.getFloFld(2));
                        
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                    
                    tuple = scan.get_next();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("Total records == "+ total_count);

    } 

    public void  printHeaderFile() throws IOException, HFException, HFBufMgrException,HFDiskMgrException, InvalidTupleSizeException {
       
        Heapfile headerFile = new Heapfile(hashIndexName);
        Scan scan = headerFile.openScan();
        RID rid = new RID();
        Tuple headerTuple = wrapperForHeader();
        Tuple r = null;
        do {

            try {
                r = (Tuple)scan.getNext(rid);
                if(r!=null) {
                    headerTuple = new Tuple(r.getTupleByteArray(), r.getOffset(), r.getLength());
                    short[] attrSize = new short[1];
        
                    attrSize[0] = REC_LEN1;
                    AttrType[] Ptypes = new AttrType[2];

                    Ptypes[0] = new AttrType (AttrType.attrInteger);
                    Ptypes[1] = new AttrType (AttrType.attrString);

                    headerTuple.setHdr((short)2, Ptypes, attrSize);

                    System.out.println("Header "+ headerTuple.getIntFld(1) + " "+ headerTuple.getStrFld(2));
                } 
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while(r!=null);
        System.out.println("Printed hashindex "+hashIndexName);

    }


    public boolean Delete(KeyClass key, RID rid) throws IOException {
        return true;
    }

    public RID search(KeyClass key) throws IOException {
        FloatKey floatKey = (FloatKey)key;
        Float keyValue = floatKey.getKey();
        int bucket = get_hash(keyValue);

        return null;
    }
    
    public int get_hash(Float value) {  
        if(split) {
            return (value.hashCode() % (2*175));
        }
        return (value.hashCode() % 175);
    }

    public void HashFileTestFunct() throws IOException {
        
    }
}
