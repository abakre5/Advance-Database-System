package tests;
//originally from : joins.C

import bufmgr.PageNotReadException;
import global.*;
import heap.*;
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

        sailors.addElement(new Sailor(1, "1", 90, 53.6));
        sailors.addElement(new Sailor(2, "2", 80, 34.2));
        sailors.addElement(new Sailor(3, "3", 78, 40.2));
        sailors.addElement(new Sailor(4, "4", 62, 39.8));
        sailors.addElement(new Sailor(5, "5", 51, 56.7));
//        sailors.addElement(new Sailor(63, "James Larus", 49, 30.3));
//        sailors.addElement(new Sailor(64, "Barton Miller", 35, 43.7));
//        sailors.addElement(new Sailor(21, "David Parter", 21, 99.9));
//        sailors.addElement(new Sailor(69, "Raghu Ramakrishnan", 19, 37.1));
//        sailors.addElement(new Sailor(35, "Guri Sohi", 10, 42.1));
//        sailors.addElement(new Sailor(73, "Prasoon Tiwari", 8, 39.2));
//        sailors.addElement(new Sailor(39, "Anne Condon", 3, 30.3));
//        sailors.addElement(new Sailor(47, "Charles Fischer", 6, 46.3));
//        sailors.addElement(new Sailor(49, "James Goodman", 4, 50.3));
//        sailors.addElement(new Sailor(50, "Mark Hill", 5, 35.2));
//        sailors.addElement(new Sailor(75, "Mary Vernon", 7, 43.1));
//        sailors.addElement(new Sailor(79, "David Wood", 3, 39.2));
//        sailors.addElement(new Sailor(84, "Mark Smucker", 9, 25.3));
//        sailors.addElement(new Sailor(87, "Martin Reames", 10, 24.1));
//        sailors.addElement(new Sailor(10, "Mike Carey", 9, 40.3));
//        sailors.addElement(new Sailor(21, "David Dewitt", 10, 47.2));
//        sailors.addElement(new Sailor(29, "Tom Reps", 7, 39.1));
//        sailors.addElement(new Sailor(31, "Jeff Naughton", 5, 35.0));
//        sailors.addElement(new Sailor(35, "Miron Livny", 7, 37.6));
//        sailors.addElement(new Sailor(37, "Marv Solomon", 10, 48.9));

        boats.addElement(new Boats(1, "Onion", "white"));
        boats.addElement(new Boats(2, "Buckey", "red"));
        boats.addElement(new Boats(3, "Enterprise", "blue"));
        boats.addElement(new Boats(4, "Voyager", "green"));
        boats.addElement(new Boats(5, "Wisconsin", "red"));


        reserves.addElement(new Reserves(5, 100, "5"));
        reserves.addElement(new Reserves(2, 80, "2"));
        reserves.addElement(new Reserves(100, 65, "1"));
        reserves.addElement(new Reserves(3, 54, "3"));
        reserves.addElement(new Reserves(4, 43, "4"));
//        reserves.addElement(new Reserves(69, 34, "05/12/95"));
//        reserves.addElement(new Reserves(70, 25, "05/14/95"));
//        reserves.addElement(new Reserves(21, 15, "05/16/95"));
//        reserves.addElement(new Reserves(57, 2, "05/10/95"));
//        reserves.addElement(new Reserves(35, 1, "05/15/95"));


        boolean status = OK;
        int numsailors = 5;
        int numsailors_attrs = 4;
        int numreserves = 5;
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

        //creating the Reserves relation
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
        Disclaimer();
        try {
            Query2();
            Query3();
        } catch (IOException | PageNotReadException | WrongPermat | JoinsException | InvalidTypeException | TupleUtilsException | UnknowAttrType | FileScanException | PredEvalException | InvalidTupleSizeException | InvalidRelation | FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        }
        System.out.print("Finished joins testing" + "\n");
        return true;
    }

    public void Query2() throws IOException, PageNotReadException, WrongPermat, JoinsException, InvalidTypeException, TupleUtilsException, UnknowAttrType, FileScanException, PredEvalException, InvalidTupleSizeException, InvalidRelation, FieldNumberOutOfBoundException {
        System.out.print("**********************Query2 strating *********************\n");
        System.out.print("Executing top K join for either INT or Float joinAttr\n");
        boolean status = OK;

        // Sailors, Boats, Reserves Queries.

//        System.out.print
//                ("Query: Find the names of sailors who have reserved a boat.\n\n"
//                        + "  SELECT S.sname\n"
//                        + "  FROM   Sailors S, Reserves R\n"
//                        + "  WHERE  S.sid = R.sid\n\n"
//                        + "(Tests FileScan, Projection, and SortMerge Join.)\n\n");

        Tuple t = new Tuple();
        t = null;

        AttrType[] Stypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrReal)
        };
        short[] Ssizes = new short[1];
        Ssizes[0] = 30;

        AttrType[] Rtypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
        };
        short[] Rsizes = new short[1];
        Rsizes[0] = 15;


        FldSpec joinAttr1 = new FldSpec(new RelSpec(RelSpec.outer), 1);
        FldSpec joinAttr2 = new FldSpec(new RelSpec(RelSpec.outer), 1);
        FldSpec mrgAttr1 = new FldSpec(new RelSpec(RelSpec.outer), 3);
        FldSpec mrgAttr2 = new FldSpec(new RelSpec(RelSpec.outer), 2);

        //This test is no longer valid as NRA does not work without index create index on table or use phase3 driver to test.
//
//        TopK_NRAJoin join = new TopK_NRAJoin(Stypes, 4, Ssizes, joinAttr1,
//                mrgAttr1, Rtypes, 3, Rsizes, joinAttr2, mrgAttr2, "sailors.in",
//                "reserves.in", 6, 100, "", );

    }
    public void Query3() throws IOException, PageNotReadException, WrongPermat, JoinsException, InvalidTypeException, TupleUtilsException, UnknowAttrType, FileScanException, PredEvalException, InvalidTupleSizeException, InvalidRelation, FieldNumberOutOfBoundException {
        System.out.print("**********************Query3 strating *********************\n");
        System.out.print("Executing top K with String as join attr");
        boolean status = OK;

        // Sailors, Boats, Reserves Queries.

//        System.out.print
//                ("Query: Find the names of sailors who have reserved a boat.\n\n"
//                        + "  SELECT S.sname\n"
//                        + "  FROM   Sailors S, Reserves R\n"
//                        + "  WHERE  S.sid = R.sid\n\n"
//                        + "(Tests FileScan, Projection, and SortMerge Join.)\n\n");

        Tuple t = new Tuple();
        t = null;

        AttrType[] Stypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrReal)
        };
        short[] Ssizes = new short[1];
        Ssizes[0] = 30;

        AttrType[] Rtypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
        };
        short[] Rsizes = new short[1];
        Rsizes[0] = 15;


        FldSpec joinAttr1 = new FldSpec(new RelSpec(RelSpec.outer), 2);
        FldSpec joinAttr2 = new FldSpec(new RelSpec(RelSpec.outer), 3);
        FldSpec mrgAttr1 = new FldSpec(new RelSpec(RelSpec.outer), 3);
        FldSpec mrgAttr2 = new FldSpec(new RelSpec(RelSpec.outer), 2);
//
//        TopK_NRAJoinString join = new TopK_NRAJoinString(Stypes, 4, Ssizes, joinAttr1,
//                mrgAttr1, Rtypes, 3, Rsizes, joinAttr2, mrgAttr2, "sailors.in",
//                "reserves.in", 6, 100 , "");

    }


    private void Disclaimer() {
        System.out.print("\n\nAny resemblance of persons in this database to"
                + " people living or dead\nis purely coincidental. The contents of "
                + "this database do not reflect\nthe views of the University,"
                + " the Computer  Sciences Department or the\n"
                + "developers...\n\n");
    }
}

public class TopKJoinNRATest {
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

