package hash;

import btree.IndexFileScan;
import global.*;
import heap.*;
import index.IndexScan;

import java.io.IOException;

public class ClusteredHashFileScan implements GlobalConst {

    ClusteredHashFile chf;
    Scan scan;
    int currentBucketIdx;
    boolean scanStarted = false;
    public ClusteredHashFileScan(ClusteredHashFile chf) throws InvalidTupleSizeException, IOException {
        assert chf != null;
        this.chf = chf;
        currentBucketIdx = 0;
        scan = chf.buckets[currentBucketIdx].openScan();
    }

    public Tuple getNext(RID rid) {
        Tuple temp = null;
        assert rid != null;

        try {
            while (temp == null) {
                temp = this.scan.getNext(rid);
                if (temp == null) {
                    this.scan.closescan();
                    ++this.currentBucketIdx;
                    if (this.currentBucketIdx < this.chf.numBuckets) {
                        this.scan = chf.buckets[currentBucketIdx].openScan();
                    } else {
                        break;
                    }
                } 
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return temp;
    }

    public void reset() throws InvalidTupleSizeException, IOException {
        currentBucketIdx = 0;
        this.scan.closescan();
        scan = chf.buckets[currentBucketIdx].openScan();
    }

    public void closeScan() throws InvalidTupleSizeException, IOException {
        reset();
    }












  public KeyDataEntry get_next() throws ScanIteratorException {
      return null;
  }
}
