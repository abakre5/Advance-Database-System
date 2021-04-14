package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;

import java.io.IOException;
import java.util.*;

import static global.AttrType.*;

public class TopK_NRAJoin {

    List<CandidateDetailsNRAReal> topKCandidate = new ArrayList<>();
    ArrayList<ArrayList<Float>> topKCandidateIndexList = new ArrayList<ArrayList<Float>>();
    AttrType[] in1;
    int len_in1;
    short[] t1_str_sizes;
    FldSpec joinAttr1;
    FldSpec mergeAttr1;
    AttrType[] in2;
    int len_in2;
    short[] t2_str_sizes;
    FldSpec joinAttr2;
    FldSpec mergeAttr2;
    String relationName1;
    String relationName2;
    int k;
    int n_pages;
    boolean relation1TuplePartOfCandidateList = false;
    boolean relation2TuplePartOfCandidateList = false;
    boolean containsDuplicates = false;

    public TopK_NRAJoin(AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1, FldSpec mergeAttr1, AttrType[] in2, int len_in2, short[] t2_str_sizes, FldSpec joinAttr2, FldSpec mergeAttr2, String relationName1, String relationName2, int k, int n_pages) throws IOException, PageNotReadException, WrongPermat, JoinsException, InvalidTypeException, TupleUtilsException, UnknowAttrType, FileScanException, PredEvalException, InvalidTupleSizeException, InvalidRelation, FieldNumberOutOfBoundException {
        this.in1 = in1;
        this.len_in1 = len_in1;
        this.t1_str_sizes = t1_str_sizes;
        this.joinAttr1 = joinAttr1;
        this.mergeAttr1 = mergeAttr1;
        this.in2 = in2;
        this.len_in2 = len_in2;
        this.t2_str_sizes = t2_str_sizes;
        this.joinAttr2 = joinAttr2;
        this.mergeAttr2 = mergeAttr2;
        this.relationName1 = relationName1;
        this.relationName2 = relationName2;
        this.k = k;
        this.n_pages = n_pages;
        try {
            computeKJoin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void determineDataTypes() {
        switch (in1[mergeAttr1.offset].attrType) {
            case attrInteger:
//                topKCandidate = new ArrayList<>();
                break;
        }
    }

    private void computeKJoin() throws Exception {
        FileScan relation1 = getFileScan(relationName1, (short) len_in1, in1, t1_str_sizes);
        FileScan relation2 = getFileScan(relationName2, (short) len_in2, in2, t2_str_sizes);
        float joinAttributeValue1 = 0;
        float joinAttributeValue2 = 0;
        float mergeAttributeValue1 = 0;
        float mergeAttributeValue2 = 0;

        if (false) {
            // if given relation does not have clustered index then return zero
        } else {
            TupleRIDPair tupleRIDRelation1 = relation1.get_next1();
            TupleRIDPair tupleRIDeRelation2 = relation2.get_next1();


            Heapfile f = new Heapfile(relationName1);
//            System.out.println("F" + f.getRecord(tupleRIDRelation1.getRID()).getIntFld(1));
//            determineDataTypes();
//            System.out.println(" " + tupleRelation1.getIntFld(1) + " " +  tupleRelation1.getStrFld(2) + " " + tupleRelation1.getIntFld(3) + " " + tupleRelation1.getFloFld(4));
//            System.out.println(" " + tupleRelation2.getIntFld(1) + " " +  tupleRelation2.getIntFld(2) + " " + tupleRelation2.getStrFld(3) );

            while (tupleRIDRelation1 != null) {
                Tuple tupleRelation1 = tupleRIDRelation1.getTuple();
                Tuple tupleRelation2 = tupleRIDeRelation2.getTuple();
                relation1TuplePartOfCandidateList = false;
                relation2TuplePartOfCandidateList = false;

                AttrType x = in1[joinAttr1.offset - 1];
                switch (x.attrType) {
                    case attrString:
                        //return "attrString";
                        break;
                    case attrInteger:
                        joinAttributeValue1 = (float) tupleRelation1.getIntFld(joinAttr1.offset);
                        joinAttributeValue2 = (float) tupleRelation2.getIntFld(joinAttr2.offset);
                        break;
                    case attrReal:
                        joinAttributeValue1 = tupleRelation1.getFloFld(joinAttr1.offset);
                        joinAttributeValue2 = tupleRelation2.getFloFld(joinAttr2.offset);
                        break;
                }

                AttrType y = in1[mergeAttr1.offset - 1];
                switch (y.attrType) {
                    case attrInteger:
                        mergeAttributeValue1 = tupleRelation1.getIntFld(mergeAttr1.offset);
                        mergeAttributeValue2 = tupleRelation2.getIntFld(mergeAttr2.offset);
                        break;
                    case attrReal:
                        mergeAttributeValue1 = tupleRelation1.getFloFld(mergeAttr1.offset);
                        mergeAttributeValue2 = tupleRelation2.getFloFld(mergeAttr2.offset);
                        break;
                }

//                System.out.println("\n join Attr 1 : " + joinAttributeValue1 + "merge Attr 1 : " + mergeAttributeValue1 +
//                        "\n join Attr 2 : " + joinAttributeValue2 + "merge Attr 2 : " + mergeAttributeValue2 );

//                for (CandidateDetailsNRAReal candidateDetailsNRAReal: topKCandidate){
//                    System.out.println("L.B : " + candidateDetailsNRAReal.getLowerBound()[0] + " "+ candidateDetailsNRAReal.getLowerBound()[1] + "\n Upper bound : " +
//                            candidateDetailsNRAReal.getUpperBound()[0] +" " +candidateDetailsNRAReal.getUpperBound()[1]);
//                }


                Set<CandidateDetailsNRAReal> duplicateCandidate = checkTupleWithTopKCandidate(joinAttributeValue1, mergeAttributeValue1, tupleRIDRelation1.getRID());
                if (duplicateCandidate.size() > 0) {
                    for (CandidateDetailsNRAReal candidateDetailsNRAReal : duplicateCandidate) {
                        addToTopKCandidateIndexList(topKCandidate.size(), candidateDetailsNRAReal.getLowerBound(), candidateDetailsNRAReal.getUpperBound());
                        topKCandidate.add(candidateDetailsNRAReal);
                    }
                    relation1TuplePartOfCandidateList = true;
                }

//                System.out.println("    ---------------------------------------------------------            ");

//                for (CandidateDetailsNRAReal candidateDetailsNRAReal: topKCandidate){
//                    System.out.println("L.B : " + candidateDetailsNRAReal.getLowerBound()[0] + " "+ candidateDetailsNRAReal.getLowerBound()[1] + "\n Upper bound : " +
//                            candidateDetailsNRAReal.getUpperBound()[0] +" " +candidateDetailsNRAReal.getUpperBound()[1]);
//                }


                //will  this IF conditions will add wrong entries to the list if both the relation have same join attribute ? - solved.
                if (!relation1TuplePartOfCandidateList) {
//                    System.out.println(tupleRIDRelation1.getRID());
                    //should not id tuple 2 Rid here we dont know the second tuple yet.
                    CandidateDetailsNRAReal tmp = new CandidateDetailsNRAReal(tupleRIDRelation1.getRID(), null, new float[]{mergeAttributeValue1, 0}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, true, false, joinAttributeValue1);
                    addToTopKCandidateIndexList(topKCandidate.size(), tmp.getLowerBound(), tmp.getUpperBound());
                    topKCandidate.add(tmp);

                }

//                System.out.println("element details after first run : " + topKCandidateIndexList + " " + topKCandidate.get(0).getLowerBound()[0] + " "+topKCandidate.get(0).getLowerBound()[1]);
//                for (CandidateDetailsNRAReal candidateDetailsNRAReal: topKCandidate){
//                    System.out.println("L.B : " + candidateDetailsNRAReal.getLowerBound()[0] + " "+ candidateDetailsNRAReal.getLowerBound()[1] + "\n Upper bound : " +
//                            candidateDetailsNRAReal.getUpperBound()[0] +" " +candidateDetailsNRAReal.getUpperBound()[1]);
//                }

                Set<CandidateDetailsNRAReal> duplicateCandidate2 = checkTupleWithTopKCandidate2(joinAttributeValue2, mergeAttributeValue2, tupleRIDeRelation2.getRID());

                if (duplicateCandidate2.size() > 0) {
                    for (CandidateDetailsNRAReal candidateDetailsNRAReal : duplicateCandidate2) {
                        addToTopKCandidateIndexList(topKCandidate.size(), candidateDetailsNRAReal.getLowerBound(), candidateDetailsNRAReal.getUpperBound());
                        topKCandidate.add(candidateDetailsNRAReal);
                    }
                    // check this assignment is correct or not.
                    relation2TuplePartOfCandidateList = true;
                }


//                checkTupleWithTopKCandidate(joinAttributeValue1, joinAttributeValue2, mergeAttributeValue1, mergeAttributeValue2);

                if (!relation2TuplePartOfCandidateList) {
                    //DOn't use default value as 1000 for upper bound use value of first tuple if complementry relation. - solved.
                    CandidateDetailsNRAReal tmp = new CandidateDetailsNRAReal(null, tupleRIDeRelation2.getRID(), new float[]{0, mergeAttributeValue2}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, false, true, joinAttributeValue2);
                    addToTopKCandidateIndexList(topKCandidate.size(), tmp.getLowerBound(), tmp.getUpperBound());
                    topKCandidate.add(tmp);
                }

//                System.out.println("number of elements in the candidate list : " + topKCandidate.size());
//                System.out.println("number of elements in the candidate list : " + topKCandidateIndexList + " " + topKCandidate.get(0).getLowerBound()[0] + " "+ topKCandidate.get(0).getLowerBound()[1]);


                topKCandidateIndexList.sort((o1, o2) -> o2.get(0).compareTo(o1.get(0)));
                List<CandidateDetailsNRAReal> topK = new ArrayList<>();

                if (topKCandidateIndexList.size() > k) {
                    if (topKCandidateIndexList.get(k - 1).get(0) > mergeAttributeValue1 + mergeAttributeValue2 && !containsDuplicates) {
                        int i = 0;
                        int count = 0;
                        for (ArrayList<Float> candidate : topKCandidateIndexList) {
                            int index = candidate.get(2).intValue();
                            if (topKCandidate.get(index).getRidRel1() == null || topKCandidate.get(index).getRidRel2() == null) {
                                i++;
                            }
                            count++;
                            if (count == k+i)
                                break;
                        }

                        if (topKCandidateIndexList.get(k +i - 1).get(0) > mergeAttributeValue1 + mergeAttributeValue2){
                            int counter = 1;
                            for (ArrayList<Float> candidate : topKCandidateIndexList) {
                                int index = candidate.get(2).intValue();
                                if (topKCandidate.get(index).getRidRel1() != null && topKCandidate.get(index).getRidRel2() != null){
                                    topK.add(topKCandidate.get(index));
                                    Tuple t1 = relation1.fetchRecord(topKCandidate.get(index).getRidRel1());
                                    Tuple rid_tuple = new Tuple(t1.getTupleByteArray(), t1.getOffset(), t1.getLength());
                                    rid_tuple.setHdr((short) in1.length, in1, t1_str_sizes);
                                    System.out.println("Printed tuple for t1 : " + rid_tuple.getLength() + " " + rid_tuple.getIntFld(1) + " " + rid_tuple.getStrFld(2) + " " + rid_tuple.getIntFld(3) + " " + rid_tuple.getFloFld(4));

//                                    System.out.println("RID for tuple 2 : " + topKCandidate.get(index).getRidRel2() + " " + topKCandidate.get(index).getRidRel2());
                                    Tuple t2 = relation2.fetchRecord(topKCandidate.get(index).getRidRel2());
                                    Tuple rid_tuple2 = new Tuple(t2.getTupleByteArray(), t2.getOffset(), t2.getLength());
                                    rid_tuple2.setHdr((short) in2.length, in2, t2_str_sizes);
                                    System.out.println("Printed tuple for tuple 2  : " + rid_tuple2.getLength() + " " + rid_tuple2.getIntFld(1) + " " + rid_tuple2.getIntFld(2) + " " + rid_tuple2.getStrFld(3));

                                    if (counter == k) {
                                        break;
                                    }
                                    counter++;
                                }

                            }
                        }
                    }
                }

                tupleRIDRelation1 = relation1.get_next1();
                tupleRIDeRelation2 = relation2.get_next1();

                if (topK.size() > 0) {
                    System.out.println("found Top k");
                    break;
                }
                System.out.println("ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg : " + topKCandidate.size());
            }
            int counter = 1;
//            for (CandidateDetailsNRAReal candidate : topKCandidate) {
//                System.out.println(candidate);
//
//            }

            for (ArrayList<Float> candidate : topKCandidateIndexList) {
                int index = candidate.get(2).intValue();

                Tuple t1 = relation1.fetchRecord(topKCandidate.get(index).getRidRel1());
                Tuple rid_tuple = new Tuple(t1.getTupleByteArray(), t1.getOffset(), t1.getLength());
                rid_tuple.setHdr((short) in1.length, in1, t1_str_sizes);
                System.out.println("after full scan Printed tuple for t1 : " + rid_tuple.getLength() + " " + rid_tuple.getIntFld(1) + " " + rid_tuple.getStrFld(2) + " " + rid_tuple.getIntFld(3) + " " + rid_tuple.getFloFld(4));

                Tuple t2 = relation2.fetchRecord(topKCandidate.get(index).getRidRel2());
                Tuple rid_tuple2 = new Tuple(t2.getTupleByteArray(), t2.getOffset(), t2.getLength());
                rid_tuple2.setHdr((short) in2.length, in2, t2_str_sizes);
                System.out.println("after full scan Printed tuple for tuple 2  : " + rid_tuple2.getLength() + " " + rid_tuple2.getIntFld(1) + " " + rid_tuple2.getIntFld(2) + " " + rid_tuple2.getStrFld(3));

                if (counter == k) {
                    break;
                }
                counter++;
            }
        }
    }

    private void printTuple(Tuple t) throws Exception {
        int num_fields = t.noOfFlds();
        for (int i = 0; i < num_fields; ++i) {
            System.out.printf("%f ", t.getFloFld(i + 1));
        }
        System.out.println("");
    }

    public void updateTopKCandidateIndexList(int index, float[] lowerBound, float[] upperBound) {
        for (ArrayList<Float> itr : topKCandidateIndexList) {
            if (itr.get(2) == index) {
                itr.set(0, lowerBound[0] + lowerBound[1]);
                itr.set(1, upperBound[0] + upperBound[1]);
            }
        }
    }

    public void addToTopKCandidateIndexList(int index, float[] lowerBound, float[] upperBound) {
        ArrayList<Float> tmp = new ArrayList<>();
        tmp.add(lowerBound[0] + lowerBound[1]);
        tmp.add(upperBound[0] + upperBound[1]);
        tmp.add((float) index);
        topKCandidateIndexList.add(tmp);
    }

    private Set<CandidateDetailsNRAReal> checkTupleWithTopKCandidate(float joinAttributeValue1, float mergeAttributeValue1, RID tuple1Rid) {
        int i = 0;
        Set<CandidateDetailsNRAReal> tmp = new LinkedHashSet<>();
        for (CandidateDetailsNRAReal candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier() == joinAttributeValue1) {
                if (!candidateDetails.isRelation1Seen()) {
                    // add one if condition to check if( setRelation1Seen != true) to handle duplicates.
                    // else part of this new if should add new entry to the list.
                    // check if u need to reset all the boolean values.
                    candidateDetails.updateUpperBound(0, mergeAttributeValue1);
                    candidateDetails.updateLowerBound(0, mergeAttributeValue1);
                    candidateDetails.setRelation1Seen(true);
                    candidateDetails.setRidRel1(tuple1Rid);
                    updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
                    relation1TuplePartOfCandidateList = true;
                    candidateFlagToUpdateTheBounds = false;
                } else if (candidateDetails.isRelation2Seen()) {
                    containsDuplicates = true;
                    float[] lowerBound = Arrays.copyOf(candidateDetails.getLowerBound(), 2);
                    lowerBound[0] = mergeAttributeValue1;
                    float[] upperBound = Arrays.copyOf(candidateDetails.getUpperBound(), 2);
                    upperBound[0] = mergeAttributeValue1;
                    CandidateDetailsNRAReal tmp1 = new CandidateDetailsNRAReal(tuple1Rid, candidateDetails.getRidRel2(),
                            lowerBound, upperBound, true, true, joinAttributeValue1);
                    tmp.add(tmp1);
                }
            }
            if (candidateFlagToUpdateTheBounds) {
                if (!candidateDetails.isRelation1Seen()) {
                    candidateDetails.updateUpperBound(0, mergeAttributeValue1);
                    updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());

                }
            }
            i++;
        }
        return tmp;
    }

//    private void checkTupleWithTopKCandidate(float joinAttributeValue1, float joinAttributeValue2, float mergeAttributeValue1, float mergeAttributeValue2  ){
//        int i = 0;
//        for (CandidateDetailsNRAReal candidateDetails : topKCandidate) {
//            boolean candidateFlagToUpdateTheBounds = true;
//            if (candidateDetails.getIdentifier() == joinAttributeValue1) {
//                // add one if condition to check if( setRelation1Seen != true) to handle duplicates.
//                // else part of this new if should add new entry to the list.
//                candidateDetails.updateUpperBound(0, mergeAttributeValue1);
//                candidateDetails.updateLowerBound(0, mergeAttributeValue1);
//                candidateDetails.setRelation1Seen(true);
//                updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
//                relation1TuplePartOfCandidateList = true;
//                candidateFlagToUpdateTheBounds = false;
//
//            }
//            if (candidateDetails.getIdentifier() == joinAttributeValue2) {
//                // add one if condition to check if( setRelation2Seen != true) to handle duplicates.
//                // further check does the old entry have relation1 data if yes you will need that data.
//                // Create new entry with new R2 and old R1 data.
//                candidateDetails.updateUpperBound(1, mergeAttributeValue2);
//                candidateDetails.updateLowerBound(1, mergeAttributeValue2);
//                candidateDetails.setRelation2Seen(true);
//                updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
//                relation2TuplePartOfCandidateList = true;
//                candidateFlagToUpdateTheBounds = false;
//            }
//            if (candidateFlagToUpdateTheBounds) {
//                if (!candidateDetails.isRelation1Seen()) {
//                    candidateDetails.updateUpperBound(0, mergeAttributeValue1);
//                }
//
//                if (!candidateDetails.isRelation2Seen()) {
//                    candidateDetails.updateUpperBound(1, mergeAttributeValue2);
//                }
//            }
//            i++;
//        }
//    }

    private Set<CandidateDetailsNRAReal> checkTupleWithTopKCandidate2(float joinAttributeValue2, float mergeAttributeValue2, RID tuple2RID) {
        int i = 0;
        Set<CandidateDetailsNRAReal> tmp = new LinkedHashSet<>();
        for (CandidateDetailsNRAReal candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier() == joinAttributeValue2) {
                if (!candidateDetails.isRelation2Seen()) {
                    // add one if condition to check if( setRelation2Seen != true) to handle duplicates.
                    // further check does the old entry have relation1 data if yes you will need that data.
                    // Create new entry with new R2 and old R1 data.
                    // check if u need to reset all the boolean values.
                    candidateDetails.updateUpperBound(1, mergeAttributeValue2);
                    candidateDetails.updateLowerBound(1, mergeAttributeValue2);
                    candidateDetails.setRelation2Seen(true);
                    candidateDetails.setRidRel2(tuple2RID);
                    updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
                    relation2TuplePartOfCandidateList = true;
                    candidateFlagToUpdateTheBounds = false;
                } else if (candidateDetails.isRelation1Seen()) {
                    containsDuplicates = true;
                    float[] lowerBound = Arrays.copyOf(candidateDetails.getLowerBound(), 2);
                    lowerBound[1] = mergeAttributeValue2;
                    float[] upperBound = Arrays.copyOf(candidateDetails.getUpperBound(), 2);
                    upperBound[1] = mergeAttributeValue2;
                    CandidateDetailsNRAReal tmp1 = new CandidateDetailsNRAReal(candidateDetails.getRidRel1(), tuple2RID,
                            lowerBound, upperBound, true, true, joinAttributeValue2);
                    tmp.add(tmp1);
                }
            }
            if (candidateFlagToUpdateTheBounds) {

                if (!candidateDetails.isRelation2Seen()) {
                    candidateDetails.updateUpperBound(1, mergeAttributeValue2);
                    updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());

                }
            }
            i++;
        }
        return tmp;
    }

    /**
     * Method Initiates the filescan on the given file Name.
     *
     * @param relationName - File name on which scan needs to be initiated.
     * @return - file scan created.
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    private FileScan getFileScan(String relationName, short noOfColumns, AttrType[] attrTypes, short[] stringSizes) throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
        FileScan scan = null;
        FldSpec[] pProjection = new FldSpec[noOfColumns];
        for (int i = 1; i <= noOfColumns; i++) {
            pProjection[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        scan = new FileScan(relationName, attrTypes, stringSizes,
                noOfColumns, noOfColumns, pProjection, null);
        return scan;
    }
}
