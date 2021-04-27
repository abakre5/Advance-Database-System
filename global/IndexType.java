package global;

/**
 * Enumeration class for IndexType
 */

public class IndexType {

    public static final int None = 0;
    public static final int B_Index = 1;
    public static final int Hash = 2;
    public static final int B_ClusteredIndex = 3;
    public static final int Clustered_Hash = 4;


    public int indexType;

    /**
     * IndexType Constructor
     * <br>
     * An index type can be defined as
     * <ul>
     * <li>   IndexType indexType = new IndexType(IndexType.Hash);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (indexType.indexType == IndexType.Hash) ....
     * </ul>
     *
     * @param _indexType The possible types of index
     */

    public IndexType(int _indexType) {
        indexType = _indexType;
    }

    public String toString() {

        switch (indexType) {
            case None:
                return "None";
            case B_Index:
                return "B_Index";
            case Hash:
                return "Hash";
            case B_ClusteredIndex:
                return  "B_ClusteredIndex";
            case Clustered_Hash:
                return "Hash_ClusteredIndex";
        }
        return ("Unexpected IndexType " + indexType);
    }

    public static String getStringForType(int type) {

        switch (type) {
            case None:
                return "_None_";
            case B_Index:
                return "_B_Index_";
            case Hash:
                return "_Hash_";
            case B_ClusteredIndex:
                return  "_btc_";
            case Clustered_Hash:
                return "_Hash_ClusteredIndex_";
        }
        return ("Unexpected IndexType " + type);
    }
}
