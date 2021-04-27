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
    private static final int    MAGICWORD       = 0xDEAD;
    private static final double UTIL_THRESHOLD  = 0.7;

    String          relationName;
    String          hashIndexName;
    int             keyAttrIndex;
    int             keyType;
    //Heapfile[]      buckets;
    int             numBucketNoSplit;
    int             multiplier = 2;
    int             numBuckets = 64;
    Heapfile        headerFile;
    boolean         split = false;
    int             maxRecordsPerBucket = 0;

    int             relNumAttrs = 0;
    AttrType[]      relAttrTypes;
    short[]         relStrSz;


  
    /**
     * ClustereHashFile constructor
     * @param RelationName:     relation on which ClusteredHashIndex is created
     * @param KeyAttrIdx:       search key attribute index (1-based)
     * @param KeyType:          search key type (AttrType.attrInteger or AttrType.attrString)
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

        try {
            RelDesc rec = new RelDesc();
            int numAttrs;

            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(RelationName, rec);
            this.relNumAttrs = numAttrs = rec.getAttrCnt();
            this.relAttrTypes = new AttrType[numAttrs];
            this.relStrSz = new short[numAttrs];
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getTupleStructure(RelationName, numAttrs, this.relAttrTypes, this.relStrSz);
        } catch (Exception e) {
            System.err.println("hereeee:" + e);
        }

        if (indexMetadataExists()) {
            //System.err.println("MD exists...!");
            readMetadata();
        } else {
            try {
                Tuple t = new Tuple();
                int tupleSize;
                int numTuplesPerPage;
                int numRecords;
                Heapfile relFile;
                int numAttrs;

                t.setHdr((short)this.relNumAttrs, this.relAttrTypes, this.relStrSz);
                tupleSize = t.size();

                numTuplesPerPage = (GlobalConst.MINIBASE_PAGESIZE - HFPage.DPFIXED) / (tupleSize);

                relFile = new Heapfile(relationName);
                numRecords = relFile.getRecCnt();

                this.maxRecordsPerBucket = (int) Math.floor(numTuplesPerPage * UTIL_THRESHOLD);
                this.numBuckets = numBucketNoSplit = (int) Math.ceil((double)numRecords / maxRecordsPerBucket);
                /*
                System.out.printf("numRecords=%d; maxRecPerBucket=%d; numBuckets=%d;  numTuplesPerPage=%d\n",
                        numRecords, this.maxRecordsPerBucket, this.numBuckets, numTuplesPerPage);
                */

                writeMetaData();
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        /*
        this.buckets = new Heapfile[this.numBuckets];
        for (int i = 0; i < this.numBuckets; ++i) {
            this.buckets[i] = getBucketFile(i);
        }
        */

	}

	private void writeMetaData() {
        try {
            Tuple t = getIndexHdrTupleStructure();
            t.setIntFld(1, MAGICWORD);
            t.setIntFld(2, numBuckets);
            t.setIntFld(3, split ? 1 : 0 );
            t.setIntFld(4, maxRecordsPerBucket);
            t.setIntFld(5, numBucketNoSplit);

            Scan scan = this.headerFile.openScan();
            Tuple temp = null;
            RID rid = new RID();

            temp = scan.getNext(rid);
            /* no tuple, exists. write one */
            if (temp == null) {
                this.headerFile.insertRecord(t.getTupleByteArray());
            } else {
                /* tuple exists, overwrite it */
                this.headerFile.updateRecord(rid, t);
            }
        } catch (Exception e) {
            System.err.println("writeMetaData: " + e);
        }
    }

    private boolean indexMetadataExists() {
        try {
            Tuple t = getIndexHdrTupleStructure();

            Scan scan = this.headerFile.openScan();
            Tuple temp = null;
            RID rid = new RID();

            temp = scan.getNext(rid);
            /* no metadata written */
            if (temp == null) {
                return false;
            } else {
                /* tuple exists, read data */
                t.tupleCopy(temp);
                int magicWord = t.getIntFld(1);
                if (magicWord != MAGICWORD) {
                    return false;
                }

                return true;
            }
        } catch (Exception e) {
            System.err.println("check_index_exists: " + e);
            return false;
        }
    }

    private void readMetadata() {
        try {
            Tuple t = getIndexHdrTupleStructure();

            Scan scan = this.headerFile.openScan();
            Tuple temp = null;
            RID rid = new RID();

            temp = scan.getNext(rid);
            /* no tuple, exists. shouldn't happen */
            if (temp == null) {
                System.err.println("hash_index: metadata missing!!!");
            } else {
                /* tuple exists, read data */
                t.tupleCopy(temp);
                int magicWord = t.getIntFld(1);
                if (magicWord != MAGICWORD) {
                    System.err.println("hash_index: metadata is corrupted!!!");
                }
                this.numBuckets = t.getIntFld(2);
                assert this.numBuckets > 0;
                this.split = t.getIntFld(3) > 1 ? true : false;
                this.maxRecordsPerBucket = t.getIntFld(4);
                assert this.maxRecordsPerBucket > 0;

                this.numBucketNoSplit = t.getIntFld(5);
                assert this.numBucketNoSplit > 0;

                scan.closescan();
                scan = null;

                /*
                System.err.printf("hash_index_readMD: numBuckets=%d; numBucketsNoSplit=%d; split=%d; multiplier=%d; maxRecsPerBucket=%d \n",
                        this.numBuckets, this.numBucketNoSplit, this.split?1:0, this.multiplier, this.maxRecordsPerBucket);

                 */

            }
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
        Heapfile bucketFile;
        KeyClass value = null;
        RID rid = new RID();

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
        bucketFile = getBucketFile(bucketIdx);


        if (bucketFile.getRecCnt() >= this.maxRecordsPerBucket) {
            /* trigger split */
            this.split = true;
            int newBucketIdx = getBucketIndex(value);

            if (newBucketIdx != bucketIdx) {
                /* move tuples that hash to new index */
                Heapfile newBucket = getBucketFile(newBucketIdx);
                Tuple tup = new Tuple();
                try {
                    tup.setHdr((short) this.relNumAttrs, this.relAttrTypes, this.relStrSz);
                    Tuple temp;
                    Scan scan = bucketFile.openScan();
                    List<RID> ridsToDelete = new ArrayList<>();

                    KeyClass tupKey = null;
                    int distribute_index;
                    while ((temp = scan.getNext(rid)) != null) {
                        tup.tupleCopy(temp);
                        if (this.keyType == AttrType.attrInteger) {
                            tupKey = new IntKey(tup.getIntFld(this.keyAttrIndex));
                        } else {
                            tupKey = new StrKey(tup.getStrFld(this.keyAttrIndex));
                        }
                        distribute_index = getBucketIndex(tupKey);
                        if (distribute_index == newBucketIdx) {
                            //bucketFile.deleteRecord(rid);
                            ridsToDelete.add(new RID(rid.pageNo, rid.slotNo));
                            newBucket.insertRecord(tup.getTupleByteArray());
                        }
                    }

                    scan.closescan();

                    for (int i = 0; i < ridsToDelete.size(); ++i) {
                        bucketFile.deleteRecord(ridsToDelete.get(i));
                    }

                    this.numBuckets++;
                    /* if num buckets is doubled, remove split logic */
                    if (this.numBuckets == 2 * this.numBucketNoSplit) {
                        this.numBucketNoSplit = this.numBuckets;
                        this.split = false;
                    }
                    writeMetaData();

                } catch (Exception e) {
                    System.err.println("insert: " + e);
                }
            }

        }

        bucketIdx = getBucketIndex(value);
        try {
            rid = getBucketFile(bucketIdx).insertRecord(t.getTupleByteArray());
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
        assert bucketIdx < (this.split? 2*this.numBucketNoSplit : this.numBucketNoSplit);

        try {
            Heapfile bucketFile = getBucketFile(bucketIdx);
            Scan scan = bucketFile.openScan();
            Tuple temp = null;
            RID rid = new RID();
            Tuple tup = new Tuple(t);
            List<RID> ridsToDelete = new ArrayList<>();
            while ((temp = scan.getNext(rid)) != null) {
                tup.tupleCopy(temp);
                if (tup.equals(t)) {
                    //System.out.println("clustered hash> found tuple to delete");
                    ridsToDelete.add(new RID(rid.pageNo, rid.slotNo));
                    //status = bucketFile.deleteRecord(rid);
                    break;
                }
            }
            scan.closescan();
            for (int i = 0; i < ridsToDelete.size(); ++i) {
                bucketFile.deleteRecord(ridsToDelete.get(i));
            }

        } catch (Exception e) {
            System.err.println("delete(): " + e);
        }
        return status;
    }

    public void printKeys() {
        Scan scan = null;
        Tuple t = null;
        AttrType[] attrTypes = null;
        short[] strSizes = null;
        Tuple temp;
        RID rid;
        KeyClass key = null;


        try {
            Heapfile rel = new Heapfile(this.relationName);
            if (rel.getRecCnt() == 0) {
                System.out.println("index is empty");
                return;
            }

            int numBuckets = (this.split) ? (2*this.numBucketNoSplit) : (this.numBucketNoSplit);

            t = new Tuple();
            t.setHdr((short)this.relNumAttrs, this.relAttrTypes, this.relStrSz);

            PageId tmpId;
            int keysInBucket;
            for (int bucketIdx = 0; bucketIdx < numBuckets; ++bucketIdx) {
                tmpId = SystemDefs.JavabaseDB.get_file_entry(getBucketFileName(bucketIdx));
                /* if bucket file does not exists, skip this bucket index */
                if (tmpId == null) {
                    continue;
                }
                keysInBucket = 0;
                scan = getBucketFile(bucketIdx).openScan();

                rid = new RID();
                while ((temp = scan.getNext(rid)) != null) {
                    if (keysInBucket == 0) {
                        System.out.println("\n\nBucket" + bucketIdx);
                    }
                    t.tupleCopy(temp);
                    if (this.keyType == AttrType.attrInteger) {
                        key = new IntKey(t.getIntFld(this.keyAttrIndex));
                    } else {
                        key = new StrKey(t.getStrFld(this.keyAttrIndex));
                    }
                    keysInBucket++;
                    System.out.print(key + "  ");
                    if ((keysInBucket % 5) == 0) {
                        System.out.println();
                    }
                }
                scan.closescan();

            }
        } catch (Exception e) {
            System.err.println("ddddd:" + e);
            e.printStackTrace();
        }
        System.out.println();
    }

    /**
     * This method returns a tuple which can be used to read/write
     * tuples from hash index header file
     */
    private Tuple getIndexHdrTupleStructure()
    {

        int numFields = 5;
        /* a bucket file tuple stores [magicword, number of buckets(int)]] */
        AttrType[] Ptypes = new AttrType[numFields];

        Ptypes[0] = new AttrType (AttrType.attrInteger); // MAGIC WORD
        Ptypes[1] = new AttrType (AttrType.attrInteger); // NUM BUCKETS
        Ptypes[2] = new AttrType (AttrType.attrInteger); // split: 1=split; 0=no split
        Ptypes[3] = new AttrType (AttrType.attrInteger); // maxRecordsPerBucket
        Ptypes[4] = new AttrType (AttrType.attrInteger); // numBucketsNoSplit
        //Ptypes[5] = new AttrType (AttrType.attrInteger); // hash function mod multiplier

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

    private String getBucketFileName(int bucketIdx) {
        return this.hashIndexName + "_" + bucketIdx;
    }
    private Heapfile getBucketFile(int bucketIdx) {
        Heapfile file = null;
        try {
            file = new Heapfile(getBucketFileName(bucketIdx));
        } catch (Exception e) {
            System.err.println(e);
        }
        return file;
    }
    private int getBucketIndex(KeyClass value) {
        int hashCode = Math.abs(value.hashCode());

        if (this.split) {
            return (hashCode % (this.multiplier*this.numBucketNoSplit));
        } else {
            return (hashCode % (this.numBucketNoSplit));
        }
    }
}
