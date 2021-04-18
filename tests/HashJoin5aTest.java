package tests;
//originally from : joins.C

import btree.BTreeFile;
import btree.IntegerKey;
import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.*;

import java.io.IOException;
import java.util.Vector;

/**
 * Here is the implementation for the tests. There are N tests performed.
 * We start off by showing that each operator works on its own.
 * Then more complicated trees are constructed.
 * As a nice feature, we allow the user to specify a selection condition.
 * We also allow the user to hardwire trees together.
 */

//Define the Sailor schema
class Sailor {
    public int sid;
    public String sname;
    public int rating;
    public double age;

    public Sailor(int _sid, String _sname, int _rating, double _age) {
        sid = _sid;
        sname = _sname;
        rating = _rating;
        age = _age;
    }
}

//Define the Boat schema
class Boats {
    public int bid;
    public String bname;
    public String color;

    public Boats(int _bid, String _bname, String _color) {
        bid = _bid;
        bname = _bname;
        color = _color;
    }
}

//Define the Reserves schema
class Reserves {
    public int sid;
    public int bid;
    public String date;

    public Reserves(int _sid, int _bid, String _date) {
        sid = _sid;
        bid = _bid;
        date = _date;
    }
}

class JoinsDriver implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector sailors;
    private Vector boats;
    private Vector reserves;

    /**
     * Constructor
     */
    public JoinsDriver() {

        //build Sailor, Boats, Reserves table
        sailors = new Vector();
        boats = new Vector();
        reserves = new Vector();

        sailors.addElement(new Sailor(53, "Bob Holloway", 9, 53.6));
        sailors.addElement(new Sailor(54, "Susan Horowitz", 1, 34.2));
        sailors.addElement(new Sailor(57, "Yannis Ioannidis", 8, 40.2));
        sailors.addElement(new Sailor(59, "Deborah Joseph", 10, 39.8));
        sailors.addElement(new Sailor(61, "Landwebber", 8, 56.7));
        sailors.addElement(new Sailor(63, "James Larus", 9, 30.3));
        sailors.addElement(new Sailor(64, "Barton Miller", 5, 43.7));
        sailors.addElement(new Sailor(67, "David Parter", 1, 99.9));
        sailors.addElement(new Sailor(69, "Raghu Ramakrishnan", 9, 37.1));
        sailors.addElement(new Sailor(71, "Guri Sohi", 10, 42.1));
        sailors.addElement(new Sailor(73, "Prasoon Tiwari", 8, 39.2));
        sailors.addElement(new Sailor(39, "Anne Condon", 3, 30.3));
        sailors.addElement(new Sailor(47, "Charles Fischer", 6, 46.3));
        sailors.addElement(new Sailor(49, "James Goodman", 4, 50.3));
        sailors.addElement(new Sailor(50, "Mark Hill", 5, 35.2));
        sailors.addElement(new Sailor(75, "Mary Vernon", 7, 43.1));
        sailors.addElement(new Sailor(79, "David Wood", 3, 39.2));
        sailors.addElement(new Sailor(84, "Mark Smucker", 9, 25.3));
        sailors.addElement(new Sailor(87, "Martin Reames", 10, 24.1));
        sailors.addElement(new Sailor(10, "Mike Carey", 9, 40.3));
        sailors.addElement(new Sailor(21, "David Dewitt", 10, 47.2));
        sailors.addElement(new Sailor(29, "Tom Reps", 7, 39.1));
        sailors.addElement(new Sailor(31, "Jeff Naughton", 5, 35.0));
        sailors.addElement(new Sailor(35, "Miron Livny", 7, 37.6));
        sailors.addElement(new Sailor(37, "Marv Solomon", 10, 48.9));

        boats.addElement(new Boats(1, "Onion", "white"));
        boats.addElement(new Boats(1, "Buckey", "red"));
        boats.addElement(new Boats(3, "Enterprise", "blue"));
        boats.addElement(new Boats(4, "Voyager", "green"));
        boats.addElement(new Boats(5, "Wisconsin", "red"));

        reserves.addElement(new Reserves(10, 1, "05/10/95"));
        reserves.addElement(new Reserves(21, 1, "05/11/95"));
        reserves.addElement(new Reserves(10, 2, "05/11/95"));
        reserves.addElement(new Reserves(31, 1, "05/12/95"));
        reserves.addElement(new Reserves(10, 3, "05/13/95"));
        reserves.addElement(new Reserves(69, 4, "05/12/95"));
        reserves.addElement(new Reserves(69, 5, "05/14/95"));
        reserves.addElement(new Reserves(21, 5, "05/16/95"));
        reserves.addElement(new Reserves(57, 2, "05/10/95"));
        reserves.addElement(new Reserves(35, 3, "05/15/95"));

        boolean status = OK;
        int numsailors = 25;
        int numsailors_attrs = 4;
        int numreserves = 10;
        int numreserves_attrs = 3;
        int numboats = 5;
        int numboats_attrs = 3;

        String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
        String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

   
    /*
    ExtendedSystemDefs extSysDef = 
      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
			      1000,500,200,"Clock");
    */

        // TODO: Revert after job completed
//        ExtendedSystemDefs sysdef = new ExtendedSystemDefs(dbpath, 1000, NUMBUF, "Clock");

        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

        // creating the sailors relation
        AttrType[] Stypes = new AttrType[4];
        Stypes[0] = new AttrType(AttrType.attrInteger);
        Stypes[1] = new AttrType(AttrType.attrString);
        Stypes[2] = new AttrType(AttrType.attrInteger);
        Stypes[3] = new AttrType(AttrType.attrReal);

        //SOS
        short[] Ssizes = new short[1];
        Ssizes[0] = 30; //first elt. is 30

        Tuple t = new Tuple();
        try {
            t.setHdr((short) 4, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "sailors"
        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("sailors.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 4, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < numsailors; i++) {
            try {
                t.setIntFld(1, ((Sailor) sailors.elementAt(i)).sid);
                t.setStrFld(2, ((Sailor) sailors.elementAt(i)).sname);
                t.setIntFld(3, ((Sailor) sailors.elementAt(i)).rating);
                t.setFloFld(4, (float) ((Sailor) sailors.elementAt(i)).age);
            } catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

        //creating the boats relation
        AttrType[] Btypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrString),
        };

        short[] Bsizes = new short[2];
        Bsizes[0] = 30;
        Bsizes[1] = 20;
        t = new Tuple();
        try {
            t.setHdr((short) 3, Btypes, Bsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        size = t.size();

        // inserting the tuple into file "boats"
        //RID             rid;
        f = null;
        try {
            f = new Heapfile("boats.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Btypes, Bsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < numboats; i++) {
            try {
                t.setIntFld(1, ((Boats) boats.elementAt(i)).bid);
                t.setStrFld(2, ((Boats) boats.elementAt(i)).bname);
                t.setStrFld(3, ((Boats) boats.elementAt(i)).color);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for boats");
            Runtime.getRuntime().exit(1);
        }

        //creating the boats relation
        AttrType[] Rtypes = new AttrType[3];
        Rtypes[0] = new AttrType(AttrType.attrInteger);
        Rtypes[1] = new AttrType(AttrType.attrInteger);
        Rtypes[2] = new AttrType(AttrType.attrString);

        short[] Rsizes = new short[1];
        Rsizes[0] = 15;
        t = new Tuple();
        try {
            t.setHdr((short) 3, Rtypes, Rsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        size = t.size();

        // inserting the tuple into file "boats"
        //RID             rid;
        f = null;
        try {
            f = new Heapfile("reserves.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Rtypes, Rsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < numreserves; i++) {
            try {
                t.setIntFld(1, ((Reserves) reserves.elementAt(i)).sid);
                t.setIntFld(2, ((Reserves) reserves.elementAt(i)).bid);
                t.setStrFld(3, ((Reserves) reserves.elementAt(i)).date);

            } catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for reserves");
            Runtime.getRuntime().exit(1);
        }

    }

    public boolean runTests() {
        System.out.println("Welcome to Hash Join testing.");
        Query9();
        System.out.print("Finished joins testing" + "\n");
        return true;
    }

    private void Query7_CondExpr(CondExpr[] expr) {
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

        expr[1] = null;
    }
    public void Query9() {
        System.out.print("********************** Query9 starting *********************\n");
        boolean status = OK;

        // Boats, Reserves Join Query.
        System.out.print("SELECT B.bname, R.date\n"
                + "  FROM   Reserves R, Boats B\n"
                + "  WHERE  R.bid = B.bid\n\n");

        System.out.print("\n(Tests Hash Join)\n");

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        Query7_CondExpr(outFilter);

        AttrType[] Btypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrString),
        };

        short[] Bsizes = new short[2];
        Bsizes[0] = 30;
        Bsizes[1] = 20;

        FldSpec[] Rprojection = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3)
        };

        AttrType[] Rtypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
        };

        short[] Rsizes = new short[1];
        Rsizes[0] = 15;

        FileScan am = null;
        try {
            am = new FileScan("reserves.in", Rtypes, Rsizes,
                    (short) 3, (short) 3,
                    Rprojection, null);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("" + e);
            e.printStackTrace();
        }

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.innerRel), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3)
        }; // B.bname, R.date

        FldSpec joinAttr1 = new FldSpec(new RelSpec(RelSpec.outer), 2);
        FldSpec joinAttr2 = new FldSpec(new RelSpec(RelSpec.outer), 1);
        FldSpec mrgAttr1 = new FldSpec(new RelSpec(RelSpec.outer), 1);
        FldSpec mrgAttr2 = new FldSpec(new RelSpec(RelSpec.outer), 1);

        HashJoin5a hjoin = null;
        try {
            hjoin = new HashJoin5a(Rtypes, 3, Rsizes,joinAttr1, mrgAttr1,
                                 Btypes, 3, Bsizes, joinAttr2, mrgAttr2, "reserves.in",
                     "boats.in", 2, 20);
        } catch (Exception e) {
            System.err.println("*** Error preparing for INLJ");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        AttrType[] jTypes = new AttrType[2];
        jTypes[0] = new AttrType(AttrType.attrString);
        jTypes[1] = new AttrType(AttrType.attrString);

        short[] jSizes = new short[2];
        jSizes[0] = 30;
        jSizes[1] = 15;

        Tuple t = new Tuple();
        try {
            t.setHdr((short) 2,jTypes, jSizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int size = t.size();
        t = new Tuple(size);
        try {
            t.setHdr((short) 2, jTypes, jSizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        int EXPECTED_COUNT = 10;
        int currentCnt = 0;
        try {
            t = hjoin.get_next();
            if( t != null ) {
                do {
//                    System.out.println("joined tuple - " + t.noOfFlds() + "  " + t.getOffset());
                    currentCnt++;
                    if( currentCnt > EXPECTED_COUNT ) {
                        System.err.println("*** WRONG RESULT: More tuples than expected");
                        status = FAIL;
                        break;
                    }
                    t = hjoin.get_next();
                } while( t != null);
            }

            if( currentCnt < EXPECTED_COUNT ) {
                System.err.println("*** WRONG RESULT: Less tuples than expected");
                status = FAIL;
            }

            hjoin.close();
        } catch (Exception e) {
            System.err.println("*** Error while getting tuples from INLJ");
            System.err.println("" + e);
            e.printStackTrace();
        }

        if (status == OK) {
            System.out.print("********************** Query9 completed successfully *********************\n");
        }
    }

}

public class HashJoin5aTest {
    public static void main(String argv[]) {
        boolean sortstatus;
        //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
        //JavabaseDB.openDB("/tmp/nwangdb", 5000);

        JoinsDriver jjoin = new JoinsDriver();

        sortstatus = jjoin.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during join tests");
        } else {
            System.out.println("join tests completed successfully");
        }
    }
}

