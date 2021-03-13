/*  File RID.java   */

package global;

import java.io.*;

/**
 * class RID
 */

public class RID {

    /**
     * public int slotNo
     */
    public int slotNo;

    /**
     * public PageId pageNo
     */
    public PageId pageNo = new PageId();

    /**
     * default constructor of class
     */
    public RID() {
    }

    /**
     * constructor of class
     */
    public RID(PageId pageno, int slotno) {
        pageNo = pageno;
        slotNo = slotno;
    }

    /**
     * make a copy of the given rid
     */
    public void copyRid(RID rid) {
        pageNo = rid.pageNo;
        slotNo = rid.slotNo;
    }

    /**
     * Write the rid into a byte array at offset
     *
     * @param ary    the specified byte array
     * @param offset the offset of byte array to write
     * @throws java.io.IOException I/O errors
     */
    public void writeToByteArray(byte[] ary, int offset)
            throws java.io.IOException {
        Convert.setIntValue(slotNo, offset, ary);
        Convert.setIntValue(pageNo.pid, offset + 4, ary);
    }

    @Override
    public boolean equals(Object rid) {

        if ((this.pageNo.pid == ((RID)rid).pageNo.pid)
                && (this.slotNo == ((RID)rid).slotNo))
            return true;
        else
            return false;
    }

    /**
     * Compares two RID object, i.e, this to the rid
     *
     * @param rid RID object to be compared to
     * @return true is they are equal
     * false if not.
     */
    public boolean equals(RID rid) {

        if ((this.pageNo.pid == rid.pageNo.pid)
                && (this.slotNo == rid.slotNo))
            return true;
        else
            return false;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int hashcode = 1;
        hashcode = prime * hashcode + pageNo.pid;
        hashcode = prime * hashcode + slotNo;
        return  hashcode;
    }



}
