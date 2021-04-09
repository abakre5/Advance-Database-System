package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

            Tuple tupleRelation1 = tupleRIDRelation1.getTuple();
            Tuple tupleRelation2 = tupleRIDeRelation2.getTuple();
            Heapfile f = new Heapfile(relationName1);
//            System.out.println("F" + f.getRecord(tupleRIDRelation1.getRID()).getIntFld(1));
//            determineDataTypes();
//            System.out.println(" " + tupleRelation1.getIntFld(1) + " " +  tupleRelation1.getStrFld(2) + " " + tupleRelation1.getIntFld(3) + " " + tupleRelation1.getFloFld(4));
//            System.out.println(" " + tupleRelation2.getIntFld(1) + " " +  tupleRelation2.getIntFld(2) + " " + tupleRelation2.getStrFld(3) );

            while (tupleRelation1 != null) {
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

                System.out.println("\n join Attr 1 : " + joinAttributeValue1 + "merge Attr 1 : " + mergeAttributeValue1 +
                        "\n join Attr 2 : " + joinAttributeValue2 + "merge Attr 2 : " + mergeAttributeValue2 );




                checkTupleWithTopKCandidate(joinAttributeValue1, mergeAttributeValue1);


                //will  this IF conditions will add wrong entries to the list if both the relation have same join attribute ?
                if (!relation1TuplePartOfCandidateList) {
                    System.out.println(tupleRIDRelation1.getRID());
                    CandidateDetailsNRAReal tmp = new CandidateDetailsNRAReal(tupleRIDRelation1.getRID(), tupleRIDeRelation2.getRID(), new float[]{mergeAttributeValue1, 0}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, true, false, joinAttributeValue1);
                    addToTopKCandidateIndexList(topKCandidate.size(), tmp.getLowerBound(), tmp.getUpperBound());
                    topKCandidate.add(tmp);

                }

                checkTupleWithTopKCandidate2( joinAttributeValue2, mergeAttributeValue2);


//                checkTupleWithTopKCandidate(joinAttributeValue1, joinAttributeValue2, mergeAttributeValue1, mergeAttributeValue2);

                if (!relation2TuplePartOfCandidateList) {
                    //DOn't use default value as 1000 for upper bound use value of first tuple if complementry relation.
                    CandidateDetailsNRAReal tmp = new CandidateDetailsNRAReal(tupleRIDRelation1.getRID(), tupleRIDeRelation2.getRID(), new float[]{0, mergeAttributeValue2}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, false, true, joinAttributeValue2);
                    addToTopKCandidateIndexList(topKCandidate.size(), tmp.getLowerBound(), tmp.getUpperBound());
                    topKCandidate.add(tmp);
                }


                topKCandidateIndexList.sort((o1, o2) -> o2.get(0).compareTo(o1.get(0)));
                List<CandidateDetailsNRAReal> topK = new ArrayList<>();

                if (topKCandidateIndexList.size() > k) {
                    if (topKCandidateIndexList.get(k - 1).get(0) > topKCandidateIndexList.get(k).get(1)) {
                        int counter = 1;
                        for (ArrayList<Float> candidate : topKCandidateIndexList) {
                            int index = candidate.get(2).intValue();
                            topK.add(topKCandidate.get(index));
//                            Tuple t1 = relation1.fetchRecord(topKCandidate.get(index).getRidRel1());
//                            System.out.println(t1);
//                            Tuple t2 = relation2.fetchRecord(topKCandidate.get(index).getRidRel2());
//                            System.out.println("No of fields " + topKCandidate.get(index).getRidRel1().pageNo + "   " + topKCandidate.get(index).getRidRel1().slotNo);
//                            System.out.println("No of fields " + t2);
//                            System.out.println(" " + t1.getIntFld(1) + " " +  t1.getStrFld(2) + " " + t1.getIntFld(3) + " " + t1.getFloFld(4));
//                            System.out.println(" " + t2.getIntFld(1) + " " +  t2.getIntFld(2) + " " + t2.getStrFld(3) );

                        System.out.println(candidate.get(0));
                            if (counter == k) {
                                break;
                            }
                            counter++;
                        }
//                        System.out.println(topK);
                    }
                }

                tupleRIDRelation1 = relation1.get_next1();
                tupleRIDeRelation2 = relation2.get_next1();

                tupleRelation1 = tupleRIDRelation1.getTuple();
                tupleRelation2 = tupleRIDeRelation2.getTuple();

                if (topK.size() > 0) {
                    System.out.println("found Top k");
                    break;
                }


            }
        }
    }

    private void printTuple(Tuple t) throws Exception {
        int num_fields = t.noOfFlds();
        for (int i=0; i < num_fields; ++i) {
            System.out.printf("%f ", t.getFloFld(i+1));
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
        tmp.add((float)index);
        topKCandidateIndexList.add(tmp);
    }
    private void checkTupleWithTopKCandidate(float joinAttributeValue1, float mergeAttributeValue1  ){
        int i = 0;
        for (CandidateDetailsNRAReal candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier() == joinAttributeValue1) {
                // add one if condition to check if( setRelation1Seen != true) to handle duplicates.
                // else part of this new if should add new entry to the list.
                candidateDetails.updateUpperBound(0, mergeAttributeValue1);
                candidateDetails.updateLowerBound(0, mergeAttributeValue1);
                candidateDetails.setRelation1Seen(true);
                updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
                relation1TuplePartOfCandidateList = true;
                candidateFlagToUpdateTheBounds = false;

            }
            if (candidateFlagToUpdateTheBounds) {
                if (!candidateDetails.isRelation1Seen()) {
                    candidateDetails.updateUpperBound(0, mergeAttributeValue1);
                    updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());

                }
            }
            i++;
        }
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

    private void checkTupleWithTopKCandidate2( float joinAttributeValue2, float mergeAttributeValue2  ){
        int i = 0;
        for (CandidateDetailsNRAReal candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier() == joinAttributeValue2) {
                // add one if condition to check if( setRelation2Seen != true) to handle duplicates.
                // further check does the old entry have relation1 data if yes you will need that data.
                // Create new entry with new R2 and old R1 data.
                candidateDetails.updateUpperBound(1, mergeAttributeValue2);
                candidateDetails.updateLowerBound(1, mergeAttributeValue2);
                candidateDetails.setRelation2Seen(true);
                updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
                relation2TuplePartOfCandidateList = true;
                candidateFlagToUpdateTheBounds = false;
            }
            if (candidateFlagToUpdateTheBounds) {

                if (!candidateDetails.isRelation2Seen()) {
                    candidateDetails.updateUpperBound(1, mergeAttributeValue2);
                    updateTopKCandidateIndexList(i, candidateDetails.getLowerBound(), candidateDetails.getUpperBound());

                }
            }
            i++;
        }
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
