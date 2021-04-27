package hash;

import java.io.*;
import java.lang.management.GarbageCollectorMXBean;
import java.security.KeyException;
import java.util.*;

import javax.management.relation.InvalidRelationTypeException;

import btree.IntegerKey;
import hash.FloatKey;
import iterator.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.TupleRIDPair;

public class HashFile extends IndexFile implements GlobalConst {

    short REC_LEN1 = 32;
    short REC_LEN2 = 160;
    AttrType[] attrType;
    AttrType[] attrs;
    short[]attrSizes;
    int numAttribs;
    boolean split = false;
    int split_position = 0;
    double threshold = 0.75;
    int num_buckets;
    int n = 0;
    double current_util = 0.0;
    int total_records = 0;
    Map<Integer,String> map = new HashMap<Integer,String>();
   //Map<String, RID> FileRIDMap = new HashMap<String, RID>();
    Heapfile headerFile = null;
    int crossed =0;
    Heapfile datafile = null;
    String hashIndexName;
    int N;
    int globalSplit = 0;
    boolean secondTry = true;
    String metadataFile = "meta";
    int stringField = 0;
    int integerField = 1;
    int floatField = 200;
    int indexkeyType;
    AttrType[] Ptypes = new AttrType[3];
    short[] entrySizes = new short[1];
    short[]attrSize;
    int keytype = 0;
    int indexAttr = -1;
    FileScan DBScan = null;
    



    /**
     * 
     * @param relationName
     * @param hashFileName
     * @param indexField
     * @param keyType
     * @param num_records
     * @param dbfile
     * @param attributeTypes
     * @param AttrSize
     * @param numAttr
     * @throws IOException
     * @throws HFException
     * @throws HFDiskMgrException
     * @throws HFBufMgrException
     * @throws InvalidTupleSizeException
     * @throws InvalidSlotNumberException
     */
    public HashFile(String relationName,String hashFileName, int indexField, int keyType,int num_records, Heapfile dbfile, AttrType[] attributeTypes, short[] AttrSize, int numAttr) throws IOException, HFException, HFDiskMgrException, HFBufMgrException,
    InvalidTupleSizeException,InvalidSlotNumberException {        

         attrs = attributeTypes;
         attrSizes = AttrSize;
         indexkeyType = keyType;
         numAttribs = numAttr;
         indexAttr = indexField;
         try{
             DBScan = getDBFileScan(relationName);
         } catch(Exception e){
             e.printStackTrace();
         }
         
        hashIndexName = hashFileName;
        datafile = dbfile;
        headerFile = new Heapfile(hashIndexName);
        metadataFile = metadataFile + "_" +hashFileName;
        indexAttr = indexField ;
        populateMap();
        //Loads the metadata file as well.
        printMetadataFile();

        //System.out.println("Loaded existing index");

    }

    //Constructor
    public HashFile(String hashFileName) throws IOException,HFException,HFBufMgrException,HFBufMgrException,HFDiskMgrException{
        hashIndexName = hashFileName;
        headerFile = new Heapfile(hashIndexName);
    }
  
    //Constructor
    /**
     * 
     * @param relationName
     * @param hashFileName
     * @param indexField
     * @param keyType
     * @param scan
     * @param num_records
     * @param dbfile
     * @param attributeTypes
     * @param AttrSize
     * @param numAttr
     * @throws IOException
     * @throws HFException
     * @throws HFDiskMgrException
     * @throws HFBufMgrException
     * @throws InvalidTupleSizeException
     * @throws InvalidSlotNumberException
     */
    public HashFile(String relationName,String hashFileName, int indexField, int keyType, FileScan scan, int num_records, Heapfile dbfile, AttrType[] attributeTypes, short[] AttrSize, int numAttr) throws IOException, HFException, HFDiskMgrException, HFBufMgrException,
                 InvalidTupleSizeException,InvalidSlotNumberException {

        attrs = attributeTypes;
        attrSizes = AttrSize;
        indexkeyType = keyType;
        numAttribs = numAttr;
        indexAttr = indexField;
        try{
            scan = getDBFileScan(relationName);
        } catch(Exception e){
            e.printStackTrace();
        }
        
        
    
        hashIndexName = hashFileName;
        datafile = dbfile;
        headerFile = new Heapfile(hashIndexName);
        metadataFile = metadataFile + "_" +hashFileName;
 
        
        //RID Wrapper Part: RID entry has fixed structure.
        Tuple t = getWrapperForRID();
        t = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
     
        entrySizes[0] = REC_LEN1;
       

        if(indexkeyType == stringField) {
            Ptypes[0] = new AttrType (AttrType.attrString);
            Ptypes[1] = new AttrType (AttrType.attrInteger);
            Ptypes[2] = new AttrType (AttrType.attrInteger);
            try {
                t.setHdr((short) 3,Ptypes, entrySizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
        } else if(indexkeyType == integerField) {
            Ptypes[0] = new AttrType (AttrType.attrInteger);
            Ptypes[1] = new AttrType (AttrType.attrInteger);
            Ptypes[2] = new AttrType (AttrType.attrInteger);
            try {
                t.setHdr((short) 3,Ptypes, entrySizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
        }
       
        int t_size = t.size();

        n =  (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / ((t_size) - 4) ;
        System.out.println(n);
        double buckets = Math.ceil(num_records / (n * threshold));
        num_buckets = (int) buckets;
        N = num_buckets;
        System.out.println("Number of buckets : "+ num_buckets);


     
        //Header file entries : Fixed format
        Tuple h = wrapperForHeader();
        int h_size = h.size();
        int numHeaderAttribs = 2;
        // This tuple corresponds to entry in a heap file.
        attrSize = new short[1];
        attrSize[0] = REC_LEN1;
        attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrString);

        try {
            h.setHdr((short)numHeaderAttribs, attrType, attrSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        //Create buckets

        for(int i =0; i<num_buckets; i++) {
            String FileName = hashIndexName+"_b_"+i;
            Heapfile hf = new Heapfile(FileName);
            map.put(i, FileName);
        }
        
        //Populate headerFile entries.
        for (Map.Entry<Integer, String> set : map.entrySet()) {
		   // System.out.println(set.getKey() + " == " + set.getValue());

            try {
                h.setIntFld(1, (int)set.getKey());
                h.setStrFld(2, set.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                headerFile.insertRecord(h.returnTupleByteArray());
                //System.out.println("Inserted");
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
        
        try {
            data.setHdr((short) numAttribs, attrs, attrSizes);
        } catch (Exception e) {
            e.printStackTrace();
        }
   
       
        int bucket=-1;
        String bucket_name = "";
       
   
        while(data !=null) {
            //System.out.println("Scanning data file for insert bucket");
            
            if(keyType == stringField) {
            String value = null;
            try {
                value = data.getStrFld(indexAttr);
                //System.out.println("Data: "+ value);
                bucket = get_string_hash(value);
                
                bucket_name = map.get(bucket);
                if(bucket_name == null) {
                //   System.out.println("Bucket Num "+bucket);
                }
                //System.out.println("Bucket Name is --> "+ bucket_name);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                
                //Setting up RID entry
                
                Heapfile bucketFile = new Heapfile(bucket_name);
                Tuple bucketEntry = new Tuple();
                
                bucketEntry.setHdr((short)3,Ptypes, entrySizes);

                bucketEntry.setStrFld(1, value);
                bucketEntry.setIntFld(2, rid.pageNo.pid);
                bucketEntry.setIntFld(3, rid.slotNo);
                // if(bucket ==0) {
                //     System.out.println("Insert debug" +value + " "+ rid.pageNo.pid + " "+rid.slotNo);
                // }
                rid = null;
                rid = bucketFile.insertRecord(bucketEntry.getTupleByteArray());
                //System.out.println("Insert debug " + bucket_name + " "+value + " "+ rid.pageNo.pid + " "+rid.slotNo);
                if(rid!=null){
                    ii++;
                }
               
                dataPair = scan.get_next1();
                if (dataPair!=null) {
                    data = dataPair.getTuple();
                    rid = dataPair.getRID();
                } else {
                    data = null;
                    //System.out.println(ii);
                }
             
                

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if(keyType == integerField) {

            Integer value = null;
            try {
               
                value = data.getIntFld(indexAttr);
                //System.out.println("Data: "+ value);
                bucket = get_int_hash(value);
                
                bucket_name = map.get(bucket);
                if(bucket_name == null) {
                    System.out.println("Null bucket name");
                }
                //System.out.println("Bucket Name is --> "+ bucket_name);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                //Setting up RID entry
                Heapfile bucketFile = new Heapfile(bucket_name);
                Tuple bucketEntry = new Tuple();
                bucketEntry.setHdr((short)3,Ptypes, entrySizes);

                bucketEntry.setIntFld(1, value);
                bucketEntry.setIntFld(2, rid.pageNo.pid);
                bucketEntry.setIntFld(3, rid.slotNo);
                // if(bucket ==0) {
                //     System.out.println("Insert debug" +value + " "+ rid.pageNo.pid + " "+rid.slotNo);
                // }
                rid = null;
                rid = bucketFile.insertRecord(bucketEntry.getTupleByteArray());
                //System.out.println("Insert debug" +value + " "+ rid.pageNo.pid + " "+rid.slotNo);
                ii++;
                dataPair = scan.get_next1();
                if (dataPair!=null) {
                    data = dataPair.getTuple();
                    rid = dataPair.getRID();
                } else {
                    data = null;
                    //System.out.println(ii);
                }
             
                

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
           
        }
        // System.out.println("Printing current state"); 
        // System.out.println(ii);
        // System.out.println(num_buckets);
        // System.out.println(n);

        double a = (N*n);
        a = ii/a;
        total_records = ii;
        current_util = a;
        // System.out.println(current_util);
        num_buckets--;
        //System.out.println("Printing hash bucket 0");


        //Dump metadta
        dumpMetadata(globalSplit, num_buckets, N, n, total_records, split_position, indexAttr);
        printMetadataFile();
        //System.out.println("========================");
        //printHeaderFile();

    
	}
    
    /**
     * 
     * @param globalSplit
     * @param num_buckets
     * @param hash_domain
     * @param tuples_per_page
     * @param total_records
     * @param split_position
     * @param indexAttr
     * @throws IOException
     * @throws HFDiskMgrException
     * @throws HFBufMgrException
     * @throws HFException
     */
    public void dumpMetadata(int globalSplit, int num_buckets, int hash_domain, 
                                int tuples_per_page, int total_records, int split_position, int indexAttr) throws IOException, HFDiskMgrException,HFBufMgrException,HFException {

        Tuple metadataTuple = wrapperForMetadata();
        AttrType[] Ptypes = new AttrType[7];
  
        Ptypes[0] = new AttrType (AttrType.attrInteger);
        Ptypes[1] = new AttrType (AttrType.attrInteger);
        Ptypes[2] = new AttrType (AttrType.attrInteger);
        Ptypes[3] = new AttrType (AttrType.attrInteger);
        Ptypes[4] = new AttrType (AttrType.attrInteger);
        Ptypes[5] = new AttrType (AttrType.attrInteger);
        Ptypes[6] = new AttrType (AttrType.attrInteger);

    
        try {
            metadataTuple.setHdr((short) 7,Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        try {
            metadataTuple.setIntFld(1, globalSplit);
            metadataTuple.setIntFld(2, num_buckets);
            metadataTuple.setIntFld(3, hash_domain);
            metadataTuple.setIntFld(4, tuples_per_page);
            metadataTuple.setIntFld(5, total_records);
            metadataTuple.setIntFld(6, split_position);
            metadataTuple.setIntFld(7, indexAttr);
        
        } catch(Exception e){
            e.printStackTrace();
        }

        Heapfile meta = new Heapfile(metadataFile);
       
        try {
            meta.deleteFile();
            meta = new Heapfile(metadataFile);
            meta.insertRecord(metadataTuple.getTupleByteArray());
          //  System.out.println("Dumped metadata file");
        } catch(Exception e) {
            e.printStackTrace();
        }

        
    }

    /**
     * 
     * @throws IOException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws HFException
     * @throws InvalidTupleSizeException
     */
    public void printMetadataFile() throws IOException, HFBufMgrException, HFDiskMgrException, HFException, InvalidTupleSizeException {
        Heapfile hf = new Heapfile(metadataFile);
        Scan scan = hf.openScan();
        RID rid = new RID();
        Tuple metaTuple = wrapperForMetadata();
        Tuple r = null;
        do {
            try {
                r = (Tuple)scan.getNext(rid);
                if(r!=null) {
                    metaTuple = new Tuple(r.getTupleByteArray(), r.getOffset(), r.getLength());

                    AttrType[] Ptypes = new AttrType[7];
  
                    Ptypes[0] = new AttrType (AttrType.attrInteger);
                    Ptypes[1] = new AttrType (AttrType.attrInteger);
                    Ptypes[2] = new AttrType (AttrType.attrInteger);
                    Ptypes[3] = new AttrType (AttrType.attrInteger);
                    Ptypes[4] = new AttrType (AttrType.attrInteger);
                    Ptypes[5] = new AttrType (AttrType.attrInteger);
                    Ptypes[6] = new AttrType (AttrType.attrInteger);
                

                    metaTuple.setHdr((short) 7,Ptypes, null);

                    // System.out.println("Metadata: "+ "globalSplit: "+  metaTuple.getIntFld(1));
                    globalSplit = metaTuple.getIntFld(1);
                    // System.out.println("Local: "+ "globalSplit: "+  globalSplit);
                    // System.out.println("Metadata: "+ "num_buckets: "+  metaTuple.getIntFld(2));
                    num_buckets = metaTuple.getIntFld(2);
                    // System.out.println("Local: "+ "num_buckets: "+  num_buckets);
                    // System.out.println("Metadata: "+ "hash domain: "+  metaTuple.getIntFld(3));
                    N = metaTuple.getIntFld(3);
                    // System.out.println("Local: "+ "N: "+  N);
                    // System.out.println("Metadata: "+ "tuples per page: "+  metaTuple.getIntFld(4));
                    n = metaTuple.getIntFld(4);
                    // System.out.println("Local: "+ "n: "+  n);
                    // System.out.println("Metadata: "+ "total records: "+  metaTuple.getIntFld(5));
                    total_records = metaTuple.getIntFld(5);
                    // System.out.println("Local: "+ "total_records: "+  total_records);
                    // System.out.println("Metadata: "+ "splitposition: "+  metaTuple.getIntFld(6));
                    split_position = metaTuple.getIntFld(6);
                    // System.out.println("Local: "+ "split_position: "+  split_position);
                    // System.out.println("Metadata: "+ "indexAttr: "+  metaTuple.getIntFld(7));
                    indexAttr = metaTuple.getIntFld(7);
                    // System.out.println("Local: "+ "indexAttr: "+  indexAttr);


                } 
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while(r!=null);
       //System.out.println("Printed metadata "+metadataFile);
        
    }

    
    /**
     * 
     * @return
     */
    public HashUnclusteredFileScan new_scan() {

        HashUnclusteredFileScan scan = new HashUnclusteredFileScan();
        scan.hfile = this;
        scan.header = this.headerFile;
        scan.header_names = new LinkedList<>();
        try{
            scan.scan = scan.header.openScan();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        try{
            populateHeaderFileMap(scan.header_names);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return scan;

    }

    /**
     * 
     * @param fname
     * @return
     * @throws IOException
     * @throws FileScanException
     * @throws TupleUtilsException
     * @throws InvalidRelation
     */
    private FileScan getFileScan(String fname) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        //File scan for buckets : Fixed format
        
        FileScan scan = null;

        FldSpec[] Pprojection = new FldSpec[3];
        for (int i = 1; i <= 3; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        AttrType[] localattrType = new AttrType[3];
        //short[] attrSize = new short[numAttribs];
        if(indexkeyType == integerField) {
            localattrType[0] = new AttrType(AttrType.attrInteger);
        } else if (indexkeyType == stringField) {
            localattrType[0] = new AttrType(AttrType.attrString);
        }
        
        localattrType[1] = new AttrType(AttrType.attrInteger);
        localattrType[2] = new AttrType(AttrType.attrInteger);
        short[] strSize = new short[2];
        strSize[0] = REC_LEN1;
        strSize[1] = REC_LEN1;
        scan = new FileScan(fname, localattrType, strSize,(short)3,3, Pprojection, null);
        return scan;
    }

    /**
     * 
     * @param fname
     * @return
     * @throws IOException
     * @throws FileScanException
     * @throws TupleUtilsException
     * @throws InvalidRelation
     */
    private FileScan getDBFileScan(String fname) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;

        FldSpec[] Pprojection = new FldSpec[3];
        for (int i = 1; i <= 3; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(fname, attrs, attrSizes,(short)numAttribs,numAttribs, Pprojection, null);
        return scan;
    }


    /**
     * 
     * @param fname
     * @return
     * @throws IOException
     * @throws FileScanException
     * @throws TupleUtilsException
     * @throws InvalidRelation
     */
    private FileScan getHeaderFileScan(String fname) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        //File scan for header file : Fixed format

        FileScan scan = null;
        FldSpec[] Pprojection = new FldSpec[2];
        for (int i = 1; i <= 2; i++) {
            Pprojection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        attrType = new AttrType[2];
        //short[] attrSize = new short[numAttribs];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrString);
    
        short[] attrSize = new short[1];
        attrSize[0] = REC_LEN1;
       
        scan = new FileScan(fname, attrType, attrSize,(short)2,2, Pprojection, null);
        return scan;
    }

      /**
     * This method returns a tuple which can be used to store rids.
     * @return
     */
    public Tuple getWrapperForRID()
    {

        // tuple for storing key, rids heapfile
        short[] attrSize = new short[1];
        AttrType[] Ptypes = new AttrType[3];
        Tuple rid_tuple = new Tuple();
        attrSize[0] = REC_LEN1;

        if(indexkeyType == stringField) {
            Ptypes[0] = new AttrType (AttrType.attrString);
            Ptypes[1] = new AttrType (AttrType.attrInteger);
            Ptypes[2] = new AttrType (AttrType.attrInteger);
            try {
                rid_tuple.setHdr((short) 3,Ptypes, attrSize);
                rid_tuple = new Tuple(rid_tuple.getTupleByteArray(), rid_tuple.getOffset(),rid_tuple.getLength());
                rid_tuple.setHdr((short) 3,Ptypes, attrSize);
                int size = rid_tuple.size();
                rid_tuple = new Tuple(size);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
        } else if(indexkeyType == integerField) {
            Ptypes[0] = new AttrType (AttrType.attrInteger);
            Ptypes[1] = new AttrType (AttrType.attrInteger);
            Ptypes[2] = new AttrType (AttrType.attrInteger);
            try {
                rid_tuple.setHdr((short) 3,Ptypes, attrSize);
                rid_tuple = new Tuple(rid_tuple.getTupleByteArray(), rid_tuple.getOffset(),rid_tuple.getLength());
                rid_tuple.setHdr((short) 3,Ptypes, attrSize);
                int size = rid_tuple.size();
                rid_tuple = new Tuple(size);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
        }
       

        return rid_tuple;

    }

    /**
     * 
     * @return
     */
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

    /**
     * 
     * @return
     */
    public Tuple wrapperForMetadata() {
        //boolean globalSplit, int num_buckets, int hash_domain, int tuples_per_page, int total_records, int split_position
         
          AttrType[] Ptypes = new AttrType[7];
  
          Ptypes[0] = new AttrType (AttrType.attrInteger);
          Ptypes[1] = new AttrType (AttrType.attrInteger);
          Ptypes[2] = new AttrType (AttrType.attrInteger);
          Ptypes[3] = new AttrType (AttrType.attrInteger);
          Ptypes[4] = new AttrType (AttrType.attrInteger);
          Ptypes[5] = new AttrType (AttrType.attrInteger);
          Ptypes[6] = new AttrType (AttrType.attrInteger);
         
  
          Tuple metadata_tuple = new Tuple();
          try {
            metadata_tuple.setHdr((short) 7,Ptypes, null);
          }
          catch (Exception e) {
              System.err.println("*** error in Tuple.setHdr() ***");
              e.printStackTrace();
          }
  
          int size = metadata_tuple.size();
          metadata_tuple = new Tuple(size);
          try {
            metadata_tuple.setHdr((short) 7, Ptypes, null);
          }
          catch (Exception e) {
              System.err.println("*** error in Tuple.setHdr() ***");
              e.printStackTrace();
          }
  
        return metadata_tuple;
    }
  
    //PlaceHolders for delete/insert.

    /**
     * 
     */
    public void insert(hash.KeyClass key, RID rid) throws IOException, FieldNumberOutOfBoundException, HFException, HFDiskMgrException, HFBufMgrException,
    InvalidBufferException,InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, FileAlreadyDeletedException {

        int index_num = -1;        
        Tuple indexEntry = getWrapperForRID();
        entrySizes[0] = REC_LEN1;
       

        if(indexkeyType == stringField) {
            //Entry for buckets : Fixed format
            Ptypes[0] = new AttrType (AttrType.attrString);
            Ptypes[1] = new AttrType (AttrType.attrInteger);
            Ptypes[2] = new AttrType (AttrType.attrInteger);
            try {
                indexEntry.setHdr((short) 3,Ptypes, entrySizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
        } else if(indexkeyType == integerField) {
            //Entry for buckets : Fixed format
            Ptypes[0] = new AttrType (AttrType.attrInteger);
            Ptypes[1] = new AttrType (AttrType.attrInteger);
            Ptypes[2] = new AttrType (AttrType.attrInteger);
            try {
                indexEntry.setHdr((short) 3,Ptypes, entrySizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }
        }

        if(indexkeyType == integerField) {
            hash.IntegerKey intkey = (hash.IntegerKey)key;
            Integer keyValue = intkey.getKey();

            try {
                indexEntry.setIntFld(1, keyValue);
                indexEntry.setIntFld(2, rid.pageNo.pid);
                indexEntry.setIntFld(3, rid.slotNo);
            } catch (Exception e) {
                e.printStackTrace();
            }
    
            index_num = get_int_hash(keyValue);
            //System.out.println("Key "+keyValue + "mapped to "+ index_num);

        } else if(indexkeyType == stringField) {
            hash.StringKey strKey = (hash.StringKey)key;
            String keyValue = strKey.getKey();
            try {
                indexEntry.setStrFld(1, keyValue);
                indexEntry.setIntFld(2, rid.pageNo.pid);
                indexEntry.setIntFld(3, rid.slotNo);
            } catch (Exception e) {
                e.printStackTrace();
            }
            index_num = get_string_hash(keyValue);
          //  System.out.println("Key "+keyValue + "mapped to "+ index_num);
        }
        
        //System.out.println("Index Num: "+index_num);

        populateMap();
        String bucket_file = map.get(index_num);
        Heapfile bucketFile = new Heapfile(bucket_file);
        
        try {
            bucketFile.insertRecord(indexEntry.getTupleByteArray());
            total_records++;
        } catch(Exception e) {
            e.printStackTrace();
        }

        double a = ((N)*n);
        a = total_records/a;        
        current_util = a;



        
        if(current_util >= threshold) {

          //  System.out.println("Target utilization has been crossed: "+current_util );
            crossed++;
            split = true;
            globalSplit = 1;
            //split_position++;
            num_buckets++;
            String FileName = hashIndexName + "_b_" + num_buckets;
            String orig_File = map.get(split_position);
            String FileName_orig_dash = orig_File+"_1";
            Heapfile hf = new Heapfile(FileName);
           // System.out.println("Creating bucket "+FileName);
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
                // System.out.println(h.getIntFld(1) + " mapped to " +h.getStrFld(2));
                // System.out.println("Header file updated");
            } catch (Exception e) {
                e.printStackTrace();
            }

            FileScan s =  null;
            Tuple tuple = null;
            try {
                s = getFileScan(orig_File);
                // System.out.println("Scanning "+orig_File);
                tuple = s.get_next();
            } catch (Exception e){
                e.printStackTrace();
            }

            while(tuple!=null) {
                //System.out.println("Redistributing");
                try{
                    if (tuple!=null) {
                        //First entry field is always key.
                        int hash2_value = -1;
                        if(indexkeyType == integerField) {
                            Integer value = tuple.getIntFld(1);
                            hash2_value = get_int_hash(value);
                            //System.out.println("Key "+value + "mapped to "+ hash2_value);
                        } else if(indexkeyType == stringField) {
                            String value = tuple.getStrFld(1);
                            hash2_value = get_string_hash(value);
                            //System.out.println("Key "+value + "mapped to "+ hash2_value);
                        }
                       
    
                        if(hash2_value == split_position) {
                            // System.out.println("Hashed to same file");
                            hf1.insertRecord(tuple.getTupleByteArray());
    
                        } else {
                            //System.out.println("Moved to new file");
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
       


        //Add entry for bucket_num_dash in header file.
        //Todo: Delete old headerfile.
        h = wrapperForHeader();
        try {
            h.setIntFld(1, split_position);
            h.setStrFld(2, FileName_orig_dash);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            headerFile.insertRecord(h.returnTupleByteArray());
            // System.out.println(h.getIntFld(1) + " dashed mapped to " +h.getStrFld(2));
            // System.out.println("Header file updated for dash bucket");
        } catch (Exception e) {
            e.printStackTrace();
        }
        split_position++;
        
        if(split_position == (N)) {
            N = 2*N;
            split = false;
            // System.out.println("Split position is as follows ->"+ split_position+" and N: "+ N);
            split_position = 0;
        
        }
        
        // System.out.println("Crossed value : "+crossed);
        try{
            deleteHeaderFileEntry(orig_File);
        } catch (Exception e){
            e.printStackTrace();
        }
    }   

    dumpMetadata(globalSplit, num_buckets, N, n, total_records, split_position, indexAttr);

}
    /**
     * 
     * @throws IOException
     * @throws InvalidTupleSizeException
     */
    public void populateMap() throws IOException, InvalidTupleSizeException {
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
                    map.put(headerTuple.getIntFld(1), headerTuple.getStrFld(2));


                } 
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while(r!=null);
        
    }

    //Test func for phase 2 data file
    public void printheapfile(String hf) throws HFBufMgrException,HFDiskMgrException,HFException,IOException, TupleUtilsException,InvalidRelation,FileScanException,FieldNumberOutOfBoundException {
        FileScan scan = getFileScan(hf);
        try {
        Tuple t = scan.get_next();
        while (t!=null) {
            System.out.println(t.getFloFld(1) + " " + t.getIntFld(2)+ " "+t.getIntFld(3));
            t = scan.get_next();
        }
    } catch(Exception e) {
        e.printStackTrace();
    }
        
    }

    /**
     * 
     * @param Name
     * @throws IOException
     * @throws HFException
     * @throws HFDiskMgrException
     * @throws HFBufMgrException
     * @throws InvalidTupleSizeException
     * @throws FileScanException
     * @throws TupleUtilsException
     * @throws InvalidRelation
     */
    public void deleteHeaderFileEntry(String Name) throws IOException, HFException,HFDiskMgrException,HFBufMgrException,InvalidTupleSizeException, 
    FileScanException,TupleUtilsException,InvalidRelation {

        Heapfile headerFile = new Heapfile(hashIndexName);
        FileScan scan = getHeaderFileScan(hashIndexName);
        TupleRIDPair r = null;
        do {

            try {
                r = scan.get_next1();
                if(r!=null) {

                    Tuple tupleOuter = r.getTuple();
                    RID ridOuter = r.getRID();
                    Tuple diskTupleToCompare = new Tuple(tupleOuter);
                   

                    //System.out.println("Delete "+ Name + " "+ diskTupleToCompare.getStrFld(2));
                    if(Name.equals(diskTupleToCompare.getStrFld(2))) {
                        headerFile.deleteRecord(ridOuter);
                        //System.out.println("Header Deleted "+ diskTupleToCompare.getIntFld(1)+" "+diskTupleToCompare.getStrFld(2));
                    }

                } 
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while(r!=null);

    }


    public HashUnclustredScan new_scan(KeyClass key) throws IOException {




        return null;
    }

    //Debug function to print the index.
    /**
     * 
     * @throws IOException
     */
    public void printindex() throws IOException {
   
        int total_count = 0;
        for (Map.Entry<Integer, String> set : map.entrySet()) {
		    //System.out.println(set.getKey() + " = " + set.getValue());
            try {
                Heapfile h = new Heapfile(set.getValue());
                //System.out.println("Printing: "+ set.getValue());
                //Heapfile data = new Heapfile("nc_2_7000_single.txt");
                total_count = total_count + h.getRecCnt();

                FileScan scan = getFileScan(set.getValue());
                Tuple tuple = scan.get_next();
                RID rid = new RID();

                while(tuple!=null) {
                    try {
                        if(indexkeyType == integerField) {
                            System.out.println("Print Index-> Key: "+ tuple.getIntFld(1) + ", "+ tuple.getIntFld(2)+ ":"+tuple.getIntFld(3));
                        } else if(indexkeyType == stringField) {
                            System.out.println("Print Index-> Key: "+ tuple.getStrFld(1) + ", "+ tuple.getIntFld(2)+ ":"+tuple.getIntFld(3));
                        }
                        
                        
                        rid.pageNo.pid = tuple.getIntFld(2);
                        rid.slotNo = tuple.getIntFld(3);
                        //writer.println("\n\n Output :"+rid.pageNo.pid + " "+rid.slotNo);
                        Tuple t = datafile.getRecord(rid);
                        Tuple current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
                        
                        current_tuple.setHdr((short)numAttribs, attrs, attrSizes);
                        //System.out.println("Data Tuple "+current_tuple.getIntFld(2) + " "+current_tuple.getIntFld(3));
                        
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                    
                    tuple = scan.get_next();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
      
        //System.out.println("Total records == "+ total_count);

    } 

    //DEBUG : Utility for printing headerfile contents
    /**
     * 
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws InvalidTupleSizeException
     */
    public void  printHeaderFile() throws IOException, HFException, HFBufMgrException,HFDiskMgrException, InvalidTupleSizeException {
       
        Heapfile headerFile = new Heapfile(hashIndexName);
        int out = 0;
        try {
            System.out.println("Headerfile elements"+ headerFile.getRecCnt());
        } catch (Exception e) {
            e.printStackTrace();
        }
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

                   // System.out.println("Header "+ headerTuple.getIntFld(1) + " "+ headerTuple.getStrFld(2));
                    Heapfile hf = new Heapfile(headerTuple.getStrFld(2));
                   // System.out.println("Count: "+ hf.getRecCnt());
                    out = out+ hf.getRecCnt();
                    //printheapfile(headerTuple.getStrFld(2));
                    // Heapfile hf = new Heapfile(headerTuple.getStrFld(2));
                    // System.out.println(hf.getRecCnt());


                } 
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while(r!=null);
        //System.out.println("Printed hashindex "+hashIndexName+ " "+out);

    }

    /**
     * 
     * @param bucketNames
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws InvalidTupleSizeException
     */
    public void  populateHeaderFileMap(Queue<String> bucketNames) throws IOException, HFException, HFBufMgrException,HFDiskMgrException, InvalidTupleSizeException {
       
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

                  //  System.out.println("Header "+ headerTuple.getIntFld(1) + " "+ headerTuple.getStrFld(2));
                    //printheapfile(headerTuple.getStrFld(2));
                    Heapfile hf = new Heapfile(headerTuple.getStrFld(2));
                    System.out.println(hf.getRecCnt());
                    bucketNames.add(headerTuple.getStrFld(2));


                } 
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while(r!=null);
        System.out.println("Printed hashindex "+ bucketNames.size()+": "+hashIndexName);

    }


    /**
     * 
     */
    public boolean delete(Tuple deleteEntry) throws IOException {

        boolean iterate = true;
        boolean deleted = false;
        KeyClass key = null;
        Tuple current_tuple = null;
        try{

        if(indexkeyType == integerField) {
            key = new hash.IntegerKey(deleteEntry.getIntFld(indexAttr));
        } else if(indexkeyType == stringField){
            key = new hash.StringKey(deleteEntry.getStrFld(indexAttr));
        }
        } catch(Exception e){
            e.printStackTrace();
        }
        if(key==null) {
            System.out.println("Null key being passed to search");
            return false;
        }

        do {

        RID rid = searchIndex(key, deleteEntry,true); 
        if (rid!=null){
            try{
                Tuple t = datafile.getRecord(rid);
                //System.out.println("Fetching matched tuple "+t);
                current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
                // AttrType[] dataFormat = new AttrType[3];
                // dataFormat[0] = new AttrType(AttrType.attrString);
                // dataFormat[1] = new AttrType(AttrType.attrInteger);
                // dataFormat[2] = new AttrType(AttrType.attrInteger);
                // short[] strdatasize = new short[1];
                // strdatasize[0] = 32;

                current_tuple.setHdr((short)numAttribs,attrs,attrSizes);

                if(TupleUtils.Equal(current_tuple, deleteEntry, attrs, numAttribs)) {
                        double a = ((N)*n);
                        a = total_records/a;        
                        current_util = a;

                        if(current_util < 0.20) {
                           // System.out.println("Triggering index shrink operation (TODO)......");
                            boolean shrink = triggerShrink();

                        }
                        dumpMetadata(globalSplit, num_buckets, N, n, total_records, split_position, indexAttr);
                        deleted = true;
                        iterate = false;
                        System.out.println("Exiting delete");
                        
            } else {
                continue;
            }
            } catch(Exception e){
                e.printStackTrace();
            }
        } else {
            System.out.println("No matching tuple");
            return false;
        }
    } while(iterate);

    return deleted;
    }

    /**
     * 
     * @param findTuple
     * @return
     * @throws IOException
     */
    public Tuple search(Tuple findTuple) throws IOException {
        boolean iterate = true;
        KeyClass key = null;
        try{

        if(indexkeyType == integerField) {
            key = new hash.IntegerKey(findTuple.getIntFld(indexAttr));
        } else if(indexkeyType == stringField){
            key = new hash.StringKey(findTuple.getStrFld(indexAttr));
        }
    } catch(Exception e){
        e.printStackTrace();
    }
        if(key==null) {
            System.out.println("Null key being passed to search");
            return null;
        }

        Tuple current_tuple = null;
        do{
        
            RID rid = searchIndex(key, findTuple,false);
        if(rid!=null){
            try{
            Tuple t = datafile.getRecord(rid);
            System.out.println("Fetching matched tuple "+t);
            current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
       
            // AttrType[] dataFormat = new AttrType[3];
            // dataFormat[0] = new AttrType(AttrType.attrString);
            // dataFormat[1] = new AttrType(AttrType.attrInteger);
            // dataFormat[2] = new AttrType(AttrType.attrInteger);
            // short[] strdatasize = new short[1];
            // strdatasize[0] = 32;

            current_tuple.setHdr((short)numAttribs,attrs,attrSizes);
            
            if(TupleUtils.Equal(findTuple, current_tuple, attrs, numAttribs)) {
                System.out.println("Found");
                iterate = false;
            } else {
                continue;
            }
            
           
        } catch(Exception e){
            e.printStackTrace();
        }
    } else {
        System.out.println("No matching tuple");
        return null;
    }

    }while(iterate);

    return current_tuple;
    }

    /**
     * 
     * @param key
     * @param searchEntry
     * @param isDelete
     * @return
     * @throws IOException
     */
    public RID searchIndex(KeyClass key, Tuple searchEntry,boolean isDelete) throws IOException {
        int bucket = -1;
        if(indexkeyType == integerField) {
            hash.IntegerKey intKey = (hash.IntegerKey)key;
            Integer keyValue = intKey.getKey();
            
            bucket = get_int_hash(keyValue);
        } else if(indexkeyType == stringField) {
            hash.StringKey strKey = (hash.StringKey)key;
            String keyValue = strKey.getKey();

            bucket = get_string_hash(keyValue);
        }

     
        Heapfile searchBucket = null;
        if (globalSplit == 1) {
          //  System.out.println("First!!");
            split = true;
        }
        String bucket_file = map.get(bucket);
       // System.out.println("Bucket Name "+ bucket + bucket_file);
        RID rid = null;
        try{
            searchBucket = new Heapfile(bucket_file);
          //  System.out.println("Total elements "+ searchBucket.getRecCnt());
            FileScan fs = getFileScan(bucket_file);

            //This checks if this bucketfile is non existant. (Yet it was mapped here by hash function because it is in domain range).
            if(bucket_file == null){
             //   System.out.println("Non existant bucket, skipping..");
                rid = null;
            } else {
                rid =  findKey(fs, searchEntry,bucket_file,key, isDelete);
            }
           
            if(rid == null && secondTry) {
                if(secondTry) {
                  //  System.out.println("Trying in another bucket");
                    globalSplit = 0;
                    split = false;
                    secondTry = false;
                    RID ridTup = searchIndex(key,searchEntry,isDelete);
                    return ridTup;
                }
                return null;
            } else if(rid == null && !secondTry) {
                System.out.println("Key not present");
                return null;

            } else {

                if(!secondTry) {
                    secondTry = true;
                    globalSplit = 1;
                    split = true;
                }
                // rid.pageNo.pid = 14;
                // rid.slotNo = 26;
                return rid;
            }

        } catch(Exception e) {
            e.printStackTrace();
        }


        return null;
    }

    /**
     * 
     * @param fs
     * @param searchEntry
     * @param bucketFile
     * @param key
     * @param isDelete
     * @return
     * @throws IOException
     * @throws JoinsException
     * @throws InvalidTupleSizeException
     */
    public RID findKey(FileScan fs, Tuple searchEntry,String bucketFile,KeyClass key, boolean isDelete) throws IOException, JoinsException, InvalidTupleSizeException{
        TupleRIDPair tupleRIDPair = null;
        Tuple tuple = null;
        RID indexRID = null;
        Integer intkey=-99999;
        String strkey=null;

        // AttrType[] dataFormat = new AttrType[3];
        // dataFormat[0] = new AttrType(AttrType.attrString);
        // dataFormat[1] = new AttrType(AttrType.attrInteger);
        // dataFormat[2] = new AttrType(AttrType.attrInteger);
        // short[] strdatasize = new short[1];
        // strdatasize[0] = 32;

        if(indexkeyType == integerField) {
            hash.IntegerKey intKey = (hash.IntegerKey)key;
            Integer keyValue = intKey.getKey();
            intkey = keyValue;
          //  System.out.println("findKey "+keyValue);
         
        } else if(indexkeyType == stringField) {
            hash.StringKey strKey = (hash.StringKey)key;
            String keyValue = strKey.getKey();
            strkey = keyValue;
           // System.out.println("findKey "+keyValue);
    
        }
        try{
            tupleRIDPair = fs.get_next1();
            indexRID = tupleRIDPair.getRID();
            Tuple localTuple = tupleRIDPair.getTuple();
            tuple = new Tuple(localTuple);
        } catch(Exception e){
            e.printStackTrace();
        }
        
        RID rid = null;
      
        while(tuple!=null) {
            try {
               
                if(indexkeyType == integerField) {
                        //System.out.println("Searching int..."+ tuple.getIntFld(1));

                        if(tuple.getIntFld(1) == intkey) {
                            //System.out.println("Key: "+ tuple.getFloFld(1) + ", "+ tuple.getIntFld(2)+ ":"+tuple.getIntFld(3));
                            rid = new RID();
                            rid.pageNo.pid = tuple.getIntFld(2);
                            rid.slotNo = tuple.getIntFld(3);

                            Tuple t = datafile.getRecord(rid);
                            Tuple current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
                            current_tuple.setHdr((short)numAttribs, attrs, attrSizes);

                            //System.out.println("DEBUG Tup: "+ current_tuple.getIntFld(2)+ ":"+current_tuple.getIntFld(3));
                            if(TupleUtils.Equal(current_tuple, searchEntry, attrs, numAttribs)) {
                            //    System.out.println("Found the entry");

                                if(isDelete) {
                                    Heapfile bucket = new Heapfile(bucketFile);
                                    boolean checkDelete = bucket.deleteRecord(indexRID);
                                    if(checkDelete) {
                                        System.out.println("Successfully deleted the entry from index file");
                                        total_records--;
                                    } else {
                                        System.out.println("Could not delete entry from index file");
                                        //writer.println("Could not delete the entry from index file");
                                        return null;
                                    }
                            }
                            return rid;
                        } else {
                            //System.out.println("Next entry");
                        }
    
                        
                    } 
                } else if(indexkeyType == stringField) {
                        //System.out.println("Searching str... "+ tuple.getStrFld(1)+ "looking for: "+strkey);
                        if(tuple.getStrFld(1).equals(strkey)) {
                            //System.out.println("Key: "+ tuple.getStrFld(1) + ", "+ tuple.getIntFld(2)+ ":"+tuple.getIntFld(3));
                            rid = new RID();
                            rid.pageNo.pid = tuple.getIntFld(2);
                            rid.slotNo = tuple.getIntFld(3);

                            Tuple t = datafile.getRecord(rid);
                            Tuple current_tuple = new Tuple(t.getTupleByteArray(), t.getOffset(),t.getLength());
                            current_tuple.setHdr((short)numAttribs, attrs, attrSizes);
                            //System.out.println("DEBUG Tup: "+ current_tuple.getIntFld(2)+ ":"+current_tuple.getIntFld(3));
                            
                            if(TupleUtils.Equal(current_tuple, searchEntry, attrs, numAttribs)) {
                                System.out.println("Found the tuple");
                                if(isDelete) {
                                    Heapfile bucket = new Heapfile(bucketFile);
                                    System.out.println();
                                    boolean checkDelete = bucket.deleteRecord(indexRID);
                                    if(checkDelete) {
                                        total_records--;
                                        System.out.println("Deleted entry from the index file");
                                        //System.out.println("Total records reduced to "+total_records);
                                    } else {
                                        System.out.println("Could not delete entry from index file");
                                        return null;
                                    }
                                }
                                
                                return rid;
                            } else {
                                //System.out.println("Next entry");
                            }
                    }

                }


               
                //System.out.println("DEBUG: Key: "+ tuple.getFloFld(1) + ", "+ tuple.getIntFld(2)+ ":"+tuple.getIntFld(3));
                tupleRIDPair = fs.get_next1();
                if(tupleRIDPair!=null) {
                    indexRID = tupleRIDPair.getRID();
                    Tuple localTuple = tupleRIDPair.getTuple();
                    tuple = new Tuple(localTuple);
                } else {
                    return null;
                }
            
            }catch(Exception e){
                e.printStackTrace();
            } 
        }

        System.out.println("Exiting Searchin loop...");
        return null;
    }
   
   
    /**
    * 
    * @param key
    * @return
    * @throws IOException
    */
    public List <RID> searchHashIndexForJoin(KeyClass key) throws IOException {
        int bucket = -1;
        List <RID> RIDList = new ArrayList<RID>();
        if(indexkeyType == integerField) {
            hash.IntegerKey intKey = (hash.IntegerKey)key;
            Integer keyValue = intKey.getKey();

            bucket = get_int_hash(keyValue);
        } else if(indexkeyType == stringField) {
            hash.StringKey strKey = (hash.StringKey)key;
            String keyValue = strKey.getKey();

            bucket = get_string_hash(keyValue);
        }

        Heapfile searchBucket = null;
        if (globalSplit == 1) {
            System.out.println("First!!");
            split = true;
        }
        String bucket_file = map.get(bucket);
        System.out.println("Bucket Name "+ bucket + bucket_file);
        RID rid = null;
        try{
            searchBucket = new Heapfile(bucket_file);
            System.out.println("Total elements "+ searchBucket.getRecCnt());
            FileScan fs = getFileScan(bucket_file);

            //This checks if this bucketfile is non existant. (Yet it was mapped here by hash function because it is in domain range).
            if(bucket_file == null){
                System.out.println("Non existant bucket, skipping..");
                RIDList = null;
            } else {
                RIDList = findKeysForJoin(fs, bucket_file,key);
            }

            if((RIDList == null || RIDList.isEmpty()) && secondTry) {
                if(secondTry) {
                    System.out.println("Trying in another bucket");
                    globalSplit = 0;
                    split = false;
                    secondTry = false;
                    List<RID> RID_List = searchHashIndexForJoin(key);
                    if (RIDList == null) {
                        RIDList.addAll(RID_List);
                    }
                    return RIDList;
                }
                return null;
            } else if((RIDList == null || RIDList.isEmpty()) && !secondTry) {
                System.out.println("Key not present");
                return RIDList;

            } else {

                if(!secondTry) {
                    secondTry = true;
                    globalSplit = 1;
                    split = true;
                }
                // rid.pageNo.pid = 14;
                // rid.slotNo = 26;
                return RIDList;
            }

        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }
   
   
   
    /**
     * 
     * @param key
     * @return
     * @throws IOException
     */
    public RID searchIndexForJoin(KeyClass key) throws IOException {
        int bucket = -1;
        if(indexkeyType == integerField) {
            hash.IntegerKey intKey = (hash.IntegerKey)key;
            Integer keyValue = intKey.getKey();

            bucket = get_int_hash(keyValue);
        } else if(indexkeyType == stringField) {
            hash.StringKey strKey = (hash.StringKey)key;
            String keyValue = strKey.getKey();

            bucket = get_string_hash(keyValue);
        }

        Heapfile searchBucket = null;
        if (globalSplit == 1) {
            System.out.println("First!!");
            split = true;
        }
        String bucket_file = map.get(bucket);
        System.out.println("Bucket Name "+ bucket + bucket_file);
        RID rid = null;
        try{
            searchBucket = new Heapfile(bucket_file);
            System.out.println("Total elements "+ searchBucket.getRecCnt());
            FileScan fs = getFileScan(bucket_file);

            //This checks if this bucketfile is non existant. (Yet it was mapped here by hash function because it is in domain range).
            if(bucket_file == null){
                System.out.println("Non existant bucket, skipping..");
                rid = null;
            } else {
                rid = findKeyForJoin(fs, bucket_file,key);
            }

            if(rid == null && secondTry) {
                if(secondTry) {
                    System.out.println("Trying in another bucket");
                    globalSplit = 0;
                    split = false;
                    secondTry = false;
                    RID ridTup = searchIndexForJoin(key);
                    return ridTup;
                }
                return null;
            } else if(rid == null && !secondTry) {
                System.out.println("Key not present");
                return null;

            } else {

                if(!secondTry) {
                    secondTry = true;
                    globalSplit = 1;
                    split = true;
                }
                // rid.pageNo.pid = 14;
                // rid.slotNo = 26;
                return rid;
            }

        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 
     * @param fs
     * @param bucketFile
     * @param key
     * @return
     * @throws IOException
     * @throws JoinsException
     * @throws InvalidTupleSizeException
     */
    public List<RID> findKeysForJoin(FileScan fs, String bucketFile, KeyClass key) throws IOException, JoinsException, InvalidTupleSizeException {
        TupleRIDPair tupleRIDPair = null;
        Tuple tuple = null;
        RID indexRID = null;
        Integer intkey=-99999;
        String strkey=null;
        List<RID> RIDList = new ArrayList<RID>();

        if(indexkeyType == integerField) {
            hash.IntegerKey intKey = (hash.IntegerKey)key;
            Integer keyValue = intKey.getKey();
            intkey = keyValue;

        } else if(indexkeyType == stringField) {
            hash.StringKey strKey = (hash.StringKey)key;
            String keyValue = strKey.getKey();
            strkey = keyValue;

        }
        try{
            tupleRIDPair = fs.get_next1();
            indexRID = tupleRIDPair.getRID();
            Tuple localTuple = tupleRIDPair.getTuple();
            tuple = new Tuple(localTuple);
        } catch(Exception e){
            e.printStackTrace();
        }

        RID rid = null;

        while(tuple!=null) {
            try {
                if(indexkeyType == integerField) {
                    if(tuple.getIntFld(1) == intkey) {
                        rid = new RID();
                        rid.pageNo.pid = tuple.getIntFld(2);
                        rid.slotNo = tuple.getIntFld(3);
                        RIDList.add(rid);
                        //return rid;
                    }
                } else if(indexkeyType == stringField) {
                    if(tuple.getStrFld(1).equals(strkey)) {
                        rid = new RID();
                        rid.pageNo.pid = tuple.getIntFld(2);
                        rid.slotNo = tuple.getIntFld(3);
                        RIDList.add(rid);
                        //return rid;
                    }
                }

                tupleRIDPair = fs.get_next1();
                if(tupleRIDPair!=null) {
                    indexRID = tupleRIDPair.getRID();
                    Tuple localTuple = tupleRIDPair.getTuple();
                    tuple = new Tuple(localTuple);
                } else {
                    break;
                }

            }catch(Exception e){
                e.printStackTrace();
            }
        }

        return RIDList;
    }


    /**
     * 
     * @param fs
     * @param bucketFile
     * @param key
     * @return
     * @throws IOException
     * @throws JoinsException
     * @throws InvalidTupleSizeException
     */
    public RID findKeyForJoin(FileScan fs, String bucketFile, KeyClass key) throws IOException, JoinsException, InvalidTupleSizeException {
        TupleRIDPair tupleRIDPair = null;
        Tuple tuple = null;
        RID indexRID = null;
        Integer intkey=-99999;
        String strkey=null;

        if(indexkeyType == integerField) {
            hash.IntegerKey intKey = (hash.IntegerKey)key;
            Integer keyValue = intKey.getKey();
            intkey = keyValue;

        } else if(indexkeyType == stringField) {
            hash.StringKey strKey = (hash.StringKey)key;
            String keyValue = strKey.getKey();
            strkey = keyValue;

        }
        try{
            tupleRIDPair = fs.get_next1();
            indexRID = tupleRIDPair.getRID();
            Tuple localTuple = tupleRIDPair.getTuple();
            tuple = new Tuple(localTuple);
        } catch(Exception e){
            e.printStackTrace();
        }

        RID rid = null;

        while(tuple!=null) {
            try {
                if(indexkeyType == integerField) {
                    if(tuple.getIntFld(1) == intkey) {
                        rid = new RID();
                        rid.pageNo.pid = tuple.getIntFld(2);
                        rid.slotNo = tuple.getIntFld(3);

                        return rid;
                    }
                } else if(indexkeyType == stringField) {
                    if(tuple.getStrFld(1).equals(strkey)) {
                        rid = new RID();
                        rid.pageNo.pid = tuple.getIntFld(2);
                        rid.slotNo = tuple.getIntFld(3);

                        return rid;
                    }
                }

                tupleRIDPair = fs.get_next1();
                if(tupleRIDPair!=null) {
                    indexRID = tupleRIDPair.getRID();
                    Tuple localTuple = tupleRIDPair.getTuple();
                    tuple = new Tuple(localTuple);
                } else {
                    return null;
                }

            }catch(Exception e){
                e.printStackTrace();
            }
        }

        return null;
    }

    public boolean triggerShrink() throws IOException{

        //Let's calculate how many buckets do we need to bring threshold to 60% if utilization has gone below 20%.
        int buckets =(int) Math.ceil(total_records/(0.60*n));
        System.out.println("Current number of buckets-> "+ num_buckets+1);        
        System.out.println("Shrinking index to-> "+ buckets);

        
        
        return true;
    }
    
    public int get_hash(Float value) {  
        if(split) {
           // System.out.println("Split hash function in action");
            return (value.hashCode() % (2*N));
        }
        return (value.hashCode() % N);
    }

    public int get_string_hash(String value) {  
        int val = get_ascii(value);
        if(split) {
           // System.out.println("Split hash function in action");
            return (val % (2*N));
        }
        return (val % N);
    }

    public int get_int_hash(Integer value) {  
        if(split) {
            //System.out.println("Split hash function in action");
            return (value % (2*N));
        }
        return (value% N);
    }

    public int get_ascii(String str) {
        int asciiValue = 0;
        for(int i=0; i<str.length(); i++) {
            asciiValue = asciiValue + str.charAt(i);
            //System.out.println(str.charAt(i) + "=" + asciiValue);
        }
        return asciiValue;
    }
    public void HashFileTestFunct() throws IOException {
        
    }
}
