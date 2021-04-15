package tests;

import bufmgr.PageNotReadException;
import catalog.AttrDesc;
import catalog.Catalogindexnotfound;
import catalog.RelDesc;
import global.AttrType;
import global.ExtendedSystemDefs;
import global.RID;
import heap.*;
import iterator.*;

import java.io.IOException;

public class IteratorDesc {

    private final FldSpec[] projlist;
    private FileScan scan;
    private short numAttr;
    private AttrType[] attrType;
    private short[] strSizes;

    public FileScan getScan() {
        return scan;
    }

    public short getNumAttr() {
        return numAttr;
    }

    public AttrType[] getAttrType() {
        return attrType;
    }

    public short[] getStrSizes() {
        return strSizes;
    }

    public FldSpec[] getProjlist() {
        return projlist;
    }

    public IteratorDesc(String fileName, short noOfAttr, AttrType[] attrType, short[] strSizes) throws IOException, FileScanException, TupleUtilsException, InvalidRelation, HFDiskMgrException, HFBufMgrException, HFException, JoinsException, FieldNumberOutOfBoundException, PageNotReadException, WrongPermat, InvalidTypeException, InvalidTupleSizeException, PredEvalException, UnknowAttrType {// create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[noOfAttr];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for (int i = 0;i < noOfAttr;i++) {
            projlist[i] = new FldSpec(rel, i + 1);
        }

        this.scan = new FileScan(fileName, attrType, strSizes, (short) attrType.length, (short) attrType.length, projlist, null);
        this.numAttr = noOfAttr;
        this.attrType = attrType;
        this.strSizes = strSizes;
        this.projlist = projlist;
    }

}
