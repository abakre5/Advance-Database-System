package hash;
import global.*;

public class KeyDataEntry {
        /**
     * key in the (key, data)
     */
    public Float key;
    /**
     * data in the (key, data)
     */
    public RID data;
    
    
    /**
     * Class constructor.
     */
    public KeyDataEntry(Float key, RID rid) {
        data = rid;
        this.key = key;
        };
    
    public KeyDataEntry() {
        
    }
      
    // //Constructor
    // public KeyDataEntry(KeyClass key, DataClass data) {
    //     if (key instanceof FloatKey){
    //         this.key = new FloatKey(((FloatKey) key).getKey());
    //     }
    //    if (data instanceof UnclusteredHashData)
    //         this.data = new UnclusteredHashData(((UnclusteredHashData) data).getData());
    // }




   

    /**
     * shallow equal.
     *
     * @param entry the entry to check again key.
     * @return true, if entry == key; else, false.
     */
    public boolean equals(KeyDataEntry entry) {
        boolean st1=false, st2=false;

        // if (key instanceof FloatKey){
        //     st1 = ((FloatKey) key).getKey().equals
        //             (((FloatKey) entry.key).getKey());
        // }
        
        // if (data instanceof UnclusteredHashData )
        //     st2 = ((RID) ((UnclusteredHashData) data).getData()).equals
        //     (((RID) ((UnclusteredHashData) entry.data).getData()));


        return (st1 && st2);
    }







}
