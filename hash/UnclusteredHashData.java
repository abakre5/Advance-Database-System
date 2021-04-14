package hash;
import global.*;

public class UnclusteredHashData extends DataClass{
    private RID myRid;


    UnclusteredHashData(RID rid) {
        myRid = new RID(rid.pageNo, rid.slotNo);
    };
    
    
    /**
     * get a copy of the rid
     *
     * @return the reference of the copy
     */
    public RID getData() {
        return new RID(myRid.pageNo, myRid.slotNo);
    };

       /**
     * set the rid
     */
    public void setData(RID rid) {
        myRid = new RID(rid.pageNo, rid.slotNo);
    };
}
