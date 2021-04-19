package hash;

import java.io.*;
import java.security.KeyException;
import java.util.*;
import java.lang.Math;

import javax.management.relation.InvalidRelationTypeException;

import hash.FloatKey;
import iterator.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.TupleRIDPair;

public class ClusteredHashFile  implements GlobalConst {
    /* class constants */
    private static final int MAGICWORD = 0xDEAD;

    String          hashIndexName;
    int             keyAttrIndex;
    int             keyType;
    int             maxExpRecords;
    Heapfile[]      buckets;
    int             numBuckets = 30;
    Heapfile        headerFile;
    final double    utilThreshold = 0.8;

  
    /**
     * ClustereHashFile constructor
     * @param RelationName:     relation on which ClusteredHashIndex is created
     * @param KeyAttrIdx:       search key attribute index (1-based)
     * @param KeyType:          search key type (AttrType.attrInteger or AttrType.attrString)
     * @param TupleSize:        Size of tuple in relation
     * @param NumRecords:       An initial estimate of number of tuples in relation
     */
    public ClusteredHashFile(String RelationName, int KeyAttrIdx, int KeyType)
            throws IOException, HFException, HFDiskMgrException, HFBufMgrException,
                 InvalidTupleSizeException,InvalidSlotNumberException {
        

        this.hashIndexName = getIndexName(RelationName, KeyAttrIdx);
        this.headerFile = new Heapfile(getIndexHdrFileName(RelationName, KeyAttrIdx));
        this.keyAttrIndex = KeyAttrIdx;
        this.keyType = KeyType;
        //this.maxExpRecords = NumRecords;
        //this.hashBucketFile = new Heapfile(this.hashIndexName);
        //headerFile = new Heapfile(hashIndexName);
        //System.out.println("Num of records = " + num_records);

        //int numTuplesPerPage =  (GlobalConst.MINIBASE_PAGESIZE - HFPage.DPFIXED) / (TupleSize) ;
        //System.out.println(n);
        //this.numBuckets = Math.max(this.numBuckets, (int) Math.ceil(NumRecords / (numTuplesPerPage * this.utilThreshold)));
        //System.out.println("hashIndexName: " + this.hashIndexName);
        //System.out.println("NumBuckets: " + this.numBuckets);

        this.buckets = new Heapfile[this.numBuckets];
        for (int i = 0; i < this.numBuckets; ++i) {
            this.buckets[i] = new Heapfile(this.hashIndexName + "_" + i);
        }

        try {
            Tuple t = getIndexHdrTupleStructure();
            t.setIntFld(1, MAGICWORD);
            t.setIntFld(2, numBuckets);
            this.headerFile.insertRecord(t.getTupleByteArray());
        } catch (Exception e) {
            System.err.println(e);
        }

	}

    private String getIndexName(String RelationName, int KeyAttrIdx) {
        return RelationName + "-clustered-hash-" + KeyAttrIdx;
    }

    private String getIndexHdrFileName(String RelationName, int KeyAttrIdx) {
        return RelationName + "-clustered-hash-" + KeyAttrIdx + ".hdr";
    }

    /**
      * This method returns a tuple which can be used to read/write
      * tuples from hash bucket file
      */
    public Tuple getBucketFileTupleStructure()
    {

        /* a bucket file tuple stores PageID of buckets' heapfiles */
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

    /**
     * This method returns a tuple which can be used to read/write
     * tuples from hash index header file
     */
    private Tuple getIndexHdrTupleStructure()
    {

        int numFields = 2;
        /* a bucket file tuple stores [magicword, number of buckets(int)]] */
        AttrType[] Ptypes = new AttrType[numFields];

        Ptypes[0] = new AttrType (AttrType.attrInteger); // MAGIC WORD
        Ptypes[1] = new AttrType (AttrType.attrInteger); // NUM BUCKETS
        //Ptypes[2] = new AttrType (AttrType.attrInteger);

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numFields, Ptypes, null);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        return t;
    }

    public RID insert(Tuple t) throws IOException, FieldNumberOutOfBoundException, HFException, HFDiskMgrException, HFBufMgrException,
    InvalidBufferException,InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, FileAlreadyDeletedException {

        int bucketIdx;
        KeyClass value = null;

        switch (this.keyType) {
            case AttrType.attrInteger: {
                value = new IntKey(t.getIntFld(this.keyAttrIndex));
                break;
            }
            case AttrType.attrReal: {
                value = new FloatKey(t.getFloFld(this.keyAttrIndex));
                break;
            }
            case AttrType.attrString: {
                value = new StrKey(t.getStrFld(this.keyAttrIndex));
                break;
            }
        }
        bucketIdx = getBucketIndex(value);
        Heapfile bucketFile = this.buckets[bucketIdx];
        RID rid = null;
        try {
            rid = bucketFile.insertRecord(t.getTupleByteArray());
        } catch(Exception e) {
            e.printStackTrace();
        }
        return rid;
    }

    public boolean delete(KeyClass Key, RID rid) throws Exception {
        boolean status = true;
        int bucketIdx;

        bucketIdx = getBucketIndex(Key);
        assert bucketIdx < this.numBuckets;
        try {
            status = this.buckets[bucketIdx].deleteRecord(rid);
        } catch (Exception e) {
            throw e;
        }
        return status;
    }

    private int getBucketIndex(KeyClass value) {
        /*
        if(split) {
            return (value.hashCode() % (2*N));
        }*/
        return (value.hashCode() % this.numBuckets);
    }
}
