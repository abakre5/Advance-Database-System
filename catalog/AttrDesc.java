//------------------------------------
// AttrDesc.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

import global.*;

// AttrDesc class: schema of attribute catalog:
public class AttrDesc {
    public String relName;                       // relation name
    public String attrName;                      // attribute name
    public int attrOffset = 0;                  // attribute offset
    public int attrPos = 0;                     // attribute position
    public AttrType attrType;                    // attribute type
    public int attrLen = 0;                     // attribute length
    public int indexCnt = 0;                    // number of indexes
    public attrData minVal = new attrData();                      // min max key values
    public attrData maxVal = new attrData();

};


