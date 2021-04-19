package hash;
import global.*;

public class KeyDataEntry {
        /**
     * key in the (key, data)
     */
    public KeyClass key;
    /**
     * data in the (key, data)
     */
    public RID data;
    
    
    /**
     * Class constructor.
     */
    public KeyDataEntry(Integer key, RID rid) {
        data = rid;
        this.key = new IntegerKey(key);
        };

    public KeyDataEntry(String key, RID rid) {
        data = rid;
        this.key = new StringKey(key);
    };
        
    
    public KeyDataEntry() {
        
    }
      
    // //Constructor
    public KeyDataEntry(KeyClass key, RID data) {
        if (key instanceof IntegerKey){
            this.key = new IntegerKey(((IntegerKey) key).getKey());
        } else if (key instanceof StringKey)
            this.key = new StringKey(((StringKey) key).getKey());  

            this.data = data;
    }





   

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
