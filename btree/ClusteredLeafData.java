package btree;

import global.*;

/**
 * ClusteredLeafData: It extends the DataClass.
 * It defines the data "pageid" for leaf node in B++ tree.
 */
public class ClusteredLeafData extends DataClass {
    private PageId pageId;

    public String toString() {
        String s;
        s = "[ " + (new Integer(pageId.pid)).toString() + " "
                 + "]";
        return s;
    }

    /**
     * Class constructor
     *
     * @param pageId - data page id
     */
    ClusteredLeafData(PageId pageId) {
        this.pageId = new PageId(pageId.pid);
    }

    ClusteredLeafData(int pageId) {
        this.pageId = new PageId(pageId);
    }

    /**
     * get a copy of the page id
     *
     * @return the reference of the copy
     */
    public PageId getData() {
        return new PageId(this.pageId.pid);
    }

    /**
     * set the page id
     */
    public void setData(PageId pageId) {
        this.pageId = new PageId(pageId.pid);
    }
}

