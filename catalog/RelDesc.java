//------------------------------------
// RelDesc.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

//   RelDesc class: schema of relation catalog:
public class RelDesc {
    String relName;                   // relation name
    int attrCnt = 0;                 // number of attributes
    int indexCnt = 0;                // number of indexed attrs
    int numTuples = 0;               // number of tuples in the relation
    int numPages = 0;                // number of pages in the file

    public int getAttrCnt() {return this.attrCnt;}
};

