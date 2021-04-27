package hash;

import java.io.*;
import java.security.KeyException;
import java.util.*;
import java.lang.Math;

import javax.management.relation.InvalidRelationTypeException;

import catalog.RelDesc;
import hash.FloatKey;
import iterator.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.TupleRIDPair;
import tests.Phase3Utils;

public class ClusteredHashFile  implements GlobalConst {
    /* class constants */
    private static final int MAGICWORD = 0xDEAD;

    String          relationName;
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
        

        this.relationName = RelationName;
        this.hashIndexName = Phase3Utils.getClusteredHashIndexName(RelationName, KeyAttrIdx);
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

    private String getIndexHdrFileName(String RelationName, int KeyAttrIdx) {
        return RelationName + "-clustered-hash-" + KeyAttrIdx + ".hdr";
    }

    /**
      * This method returns a tuple which can be used to read/write
      * tuples from hash bucket file
      */
    private Tuple getBucketFileTupleStructure()
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
        //System.err.println("insert> bucketNum: " + bucketIdx);
        Heapfile bucketFile = this.buckets[bucketIdx];
        RID rid = null;
        try {
            rid = bucketFile.insertRecord(t.getTupleByteArray());
        } catch(Exception e) {
            e.printStackTrace();
        }
        return rid;
    }

    public boolean delete(Tuple t) throws Exception {
        boolean status = true;
        int bucketIdx;
        KeyClass key = null;

        if (this.keyType == AttrType.attrInteger) {
            key = new IntKey(t.getIntFld(this.keyAttrIndex));
        } else {
            key = new StrKey(t.getStrFld(this.keyAttrIndex));
        }

        bucketIdx = getBucketIndex(key);
        //System.err.println("delete> bucketNum: " + bucketIdx);
        assert bucketIdx < this.numBuckets;
        try {
            Heapfile bucketFile = this.buckets[bucketIdx];
            Scan scan = bucketFile.openScan();
            Tuple temp = null;
            RID rid = new RID();
            Tuple tup = new Tuple(t);
            while ((temp = scan.getNext(rid)) != null) {
                tup.tupleCopy(temp);
                if (tup.equals(t)) {
                    //System.out.println("clustered hash> found tuple to delete");
                    status = bucketFile.deleteRecord(rid);
                    //scan.closescan();
                    break;
                }
            }
        } catch (Exception e) {
            //throw e;
        }
        return status;
    }

    public void printKeys() {
        Scan scan = null;
        Tuple t = null;
        AttrType[] attrTypes = null;
        short[] strSizes = null;
        try {
            int numAttribs;
            RelDesc rec = new RelDesc();
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(this.relationName, rec);
            numAttribs = rec.getAttrCnt();
            attrTypes = new AttrType[numAttribs];
            strSizes = new short[numAttribs];

            ExtendedSystemDefs.MINIBASE_ATTRCAT.getTupleStructure(this.relationName, numAttribs, attrTypes, strSizes);
            t = new Tuple();
            t.setHdr((short)numAttribs, attrTypes, strSizes);
        } catch (Exception e) {
            System.err.println("*** error fetching catalog info");
            return;
        }
        Tuple temp;
        RID rid = new RID();
        KeyClass key = null;
        try {
            for (int bucketIdx = 0; bucketIdx < this.numBuckets; ++bucketIdx) {
                scan = this.buckets[bucketIdx].openScan();
                while ((temp = scan.getNext(rid)) != null) {
                    t.tupleCopy(temp);
                    if (this.keyType == AttrType.attrInteger) {
                        key = new IntKey(t.getIntFld(this.keyAttrIndex));
                    } else {
                        key = new StrKey(t.getStrFld(this.keyAttrIndex));
                    }
                    System.out.println(key);
                }
                scan.closescan();

            }
        } catch (Exception e) {
            System.err.println(e);
        }
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

    private int getBucketIndex(KeyClass value) {
        /*
        if(split) {
            return (value.hashCode() % (2*N));
        }*/
        return (Math.abs(value.hashCode()) % this.numBuckets);
    }
}
