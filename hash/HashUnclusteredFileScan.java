package hash;

import global.*;
import heap.*;
import iterator.FileScan;


import java.util.List;
import java.util.Queue;
import java.util.ArrayList;

public class HashUnclusteredFileScan extends HashIndexFileScan implements GlobalConst{

    HashFile hfile;
    Scan scan = null;
    Scan bucket_scan = null;
    int bucket_entry = 0;
    KeyDataEntry entry = new KeyDataEntry();
    Heapfile header = null;
    boolean is_bucket_completed = false;
    boolean is_first_header_scan = true;
    boolean is_first_bucket_scan = true;
    Queue <String> header_names = null;
    int currentBucketCnt = 0;
    String currentBucketName;
    Heapfile bucket = null;
    int bucketEntryCnt = 0;
    FileScan bucketFileScan = null;
    RID bucketRID = new RID();
    RID bucketNextRID = new RID();
    
    public KeyDataEntry get_next() throws ScanIteratorException{
        if(header_names.isEmpty()){
            return null;
        }

       
    
        if(is_first_bucket_scan && !header_names.isEmpty()) {
        try {
            while(bucketEntryCnt==0) {
                if(header_names.isEmpty()) {
                    return null;
                }
            currentBucketName = header_names.peek();
            header_names.size();
            //System.out.println("Scanning Size "+header_names.size() +currentBucketName);
            bucket = new Heapfile(currentBucketName);
            bucketEntryCnt = bucket.getRecCnt();
            if(bucketEntryCnt!=0) {
                //System.out.println(currentBucketName + " ======="+ bucketEntryCnt + "elements");
            }
        
            bucket_scan = bucket.openScan();
            bucketRID = new RID();
            is_first_bucket_scan = false;
            if(bucketEntryCnt == 0){
                header_names.remove();

            }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
        Tuple ridTup = hfile.getWrapperForRID();
        Tuple ridTuple = new Tuple(ridTup.getTupleByteArray(), ridTup.getOffset(),ridTup.getLength());

        // This tuple corresponds to entry in a heap file.
        AttrType[] attrType = new AttrType[3];
        //short[] attrSize = new short[numAttribs];

        short[] attrSizes;
        attrSizes = hfile.attrSizes;


        try {
            ridTuple = bucket_scan.getNext(bucketRID);
            if(ridTuple!=null) {
                
                //System.out.println("Bucket Entry Cnt "+ bucketEntryCnt);
                bucketEntryCnt--;
                //System.out.println("Index Attr "+hfile.indexAttr);
                if(hfile.indexkeyType == hfile.integerField) {
                    attrType[0] = new AttrType (AttrType.attrInteger);
                    attrType[1] = new AttrType (AttrType.attrInteger);
                    attrType[2] = new AttrType (AttrType.attrInteger);
                    
                    ridTuple.setHdr((short)3, attrType, attrSizes);
                    entry.key = new IntegerKey(ridTuple.getIntFld(1));
                    // System.out.println("Scan Key Integer"+ ridTuple.getIntFld(1));
                    // System.out.println("RID "+ ridTuple.getIntFld(2)+ ":"+ ridTuple.getIntFld(3));
                } else if (hfile.indexkeyType == hfile.stringField){
                    attrType[0] = new AttrType (AttrType.attrString);
                    attrType[1] = new AttrType (AttrType.attrInteger);
                    attrType[2] = new AttrType (AttrType.attrInteger);

                    ridTuple.setHdr((short)3, attrType, attrSizes);
                    entry.key = new StringKey(ridTuple.getStrFld(1));
                    // System.out.println("Scan Key Str"+ ridTuple.getStrFld(1));
                    // System.out.println("RID "+ ridTuple.getIntFld(2)+ ":"+ ridTuple.getIntFld(3));
                }
                
                RID insert_rid = new RID();
                insert_rid.pageNo.pid = ridTuple.getIntFld(2);
                insert_rid.slotNo = ridTuple.getIntFld(3);
                //System.out.println("PID:SLOT "+insert_rid.pageNo.pid+":"+insert_rid.slotNo);
                entry.data = new RID(insert_rid.pageNo,insert_rid.slotNo);
                
                if(bucketEntryCnt == 0) {
                   // System.out.println("This is the last entry");
                    header_names.remove();
                    
                    currentBucketCnt++;
                    bucketRID = new RID();
                    is_first_bucket_scan = true;
                    return entry;
                }
                
                if(entry == null){
                //   System.out.println("Null entry");
                    return null;
                }
                return entry;   
                    
            } 
            
        } catch(Exception e) {
            e.printStackTrace();
        }
        //System.out.println("Returned from here");
        return null;
    }

    public void delete_current(){

    }

    public int keysize() {
        return 99;
    }
    
}
