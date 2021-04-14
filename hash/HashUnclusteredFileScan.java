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

        currentBucketName = header_names.peek();
        //header_names.size();

        System.out.println("Scanning Size "+header_names.size() +currentBucketName);
    
        if(is_first_bucket_scan) {
        try {
            bucket = new Heapfile(currentBucketName);
            bucketEntryCnt = bucket.getRecCnt();
            System.out.println(currentBucketName + "has "+ bucketEntryCnt + "elements");
            bucket_scan = bucket.openScan();
            bucketRID = new RID();
            is_first_bucket_scan = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
        Tuple ridTup = hfile.getWrapperForRID();
        Tuple ridTuple = new Tuple(ridTup.getTupleByteArray(), ridTup.getOffset(),ridTup.getLength());

        // This tuple corresponds to entry in a heap file.
        AttrType[] attrType = new AttrType[3];
        //short[] attrSize = new short[numAttribs];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSizes = new short[2];
        attrSizes[0] = hfile.REC_LEN1;
        attrSizes[1] = hfile.REC_LEN1;

        try {
            ridTuple = bucket_scan.getNext(bucketRID);
            if(ridTuple!=null) {
                ridTuple.setHdr((short)3, attrType, attrSizes);
                System.out.println("Bucket Entry Cnt "+ bucketEntryCnt);
                bucketEntryCnt--;
                entry.key = ridTuple.getFloFld(1);
                RID insert_rid = new RID();
                insert_rid.pageNo.pid = ridTuple.getIntFld(2);
                insert_rid.slotNo = ridTuple.getIntFld(3);
                entry.data = new RID(insert_rid.pageNo,insert_rid.slotNo);
                
                // bucketNextRID.pageNo.pid = bucketRID.pageNo.pid;
                // bucketNextRID.slotNo = bucketRID.slotNo;
                // nextTuple = bucket_scan.getNext(bucketNextRID);
                // if(nextTuple == null) {
                //     System.out.println("This is the last entry");
                //     header_names.remove(currentBucketCnt);
                //     currentBucketCnt++;
                //     bucketRID = new RID();
                //     is_first_bucket_scan = true;
                // }
                
                if(entry == null){
                    System.out.println("Null entry");
                    return null;
                }
                return entry;   
                    
            } else {
                    System.out.println("Here is the last entry");
                    header_names.remove();
                    currentBucketCnt++;
                    is_first_bucket_scan = true;
                    return entry;

            }

           
            
        } catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("Returned from here");
        return null;
    }

    public void delete_current(){

    }

    public int keysize() {
        return 99;
    }
    
}
