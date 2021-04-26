package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.*;
import index.IndexScan;

import java.io.IOException;
import java.util.*;

import static global.AttrType.*;

public class TopK_NRAJoinString {

    List<CandidateDetailsNRAString> topKCandidate = new ArrayList<>();
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
    Heapfile materialisedTable = null;
    String materialisedTableName = "";
    IndexScan outerIndexScan;
    IndexScan innerIndexScan;


    public TopK_NRAJoinString(AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1, FldSpec mergeAttr1, AttrType[] in2, int len_in2, short[] t2_str_sizes, FldSpec joinAttr2, FldSpec mergeAttr2, String relationName1, String relationName2, int k, int n_pages, String materialisedTableName, IndexScan outerIndexScan, IndexScan innerIndexScan) throws IOException, PageNotReadException, WrongPermat, JoinsException, InvalidTypeException, TupleUtilsException, UnknowAttrType, FileScanException, PredEvalException, InvalidTupleSizeException, InvalidRelation, FieldNumberOutOfBoundException {
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
        this.outerIndexScan = outerIndexScan;
        this.innerIndexScan = innerIndexScan;
        if(!materialisedTableName.equals("")){
            try {
                this.materialisedTableName = materialisedTableName;
                this.materialisedTable  = new Heapfile(materialisedTableName);
            } catch (HFException | HFBufMgrException | HFDiskMgrException e) {
                System.out.println("File creation for materialised file failed.");
                e.printStackTrace();
            }
        }
        try {
            computeKJoin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void computeKJoin() throws Exception {
        IndexScan relation1 = outerIndexScan;
        FileScan relation3 = getFileScan(relationName1, (short) len_in1, in1, t1_str_sizes);
        IndexScan relation2 = innerIndexScan;
        FileScan relation4 = getFileScan(relationName2, (short) len_in2, in2, t2_str_sizes);
        String joinAttributeValue1 = null;
        String joinAttributeValue2 = null;
        float mergeAttributeValue1 = 0;
        float mergeAttributeValue2 = 0;
        FldSpec[] proj_list = new FldSpec[len_in1 + len_in2];
//        System.out.println("con=ming till here ");
        for (int i = 1; i <= len_in1; i++) {
            proj_list[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        for (int i = 1; i <= len_in2; i++) {
            proj_list[len_in1 + i - 1] = new FldSpec(new RelSpec(RelSpec.innerRel),  i);
        }

        if (false) {
            // if given relation does not have clustered index then return zero
        } else {
            TupleRIDPair tupleRIDRelation1 = relation1.get_next1();
            TupleRIDPair tupleRIDRelation2 = relation2.get_next1();

            while (tupleRIDRelation1 != null || tupleRIDRelation2 != null) {
                Tuple tupleRelation1;
                Tuple tupleRelation2;
                if (tupleRIDRelation1 != null) {
                    tupleRelation1 = tupleRIDRelation1.getTuple();
                    AttrType x = in1[joinAttr1.offset - 1];
                    if (x.attrType == attrString) {
                        joinAttributeValue1 = tupleRelation1.getStrFld(joinAttr1.offset);
                    }

                    AttrType y = in1[mergeAttr1.offset - 1];
                    switch (y.attrType) {
                        case attrInteger:
                            mergeAttributeValue1 = tupleRelation1.getIntFld(mergeAttr1.offset);
                            break;
                        case attrReal:
                            mergeAttributeValue1 = tupleRelation1.getFloFld(mergeAttr1.offset);
                            break;
                    }
                }
                if (tupleRIDRelation2 != null) {
                    tupleRelation2 = tupleRIDRelation2.getTuple();
                    AttrType x = in1[joinAttr1.offset - 1];
                    if (x.attrType == attrString) {
                        joinAttributeValue2 = tupleRelation2.getStrFld(joinAttr2.offset);
                    }

                    AttrType y = in1[mergeAttr1.offset - 1];
                    switch (y.attrType) {
                        case attrInteger:
                            mergeAttributeValue2 = tupleRelation2.getIntFld(mergeAttr2.offset);
                            break;
                        case attrReal:
                            mergeAttributeValue2 = tupleRelation2.getFloFld(mergeAttr2.offset);
                            break;
                    }
                }
                relation1TuplePartOfCandidateList = false;
                relation2TuplePartOfCandidateList = false;

                if (tupleRIDRelation1 != null) {
                    Set<CandidateDetailsNRAString> duplicateCandidate = checkTupleWithTopKCandidate(joinAttributeValue1, mergeAttributeValue1, tupleRIDRelation1.getRID());
                    if (duplicateCandidate.size() > 0) {
                        for (CandidateDetailsNRAString candidateDetails : duplicateCandidate) {
                            addToTopKCandidateIndexList(topKCandidate.size(), candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
                            topKCandidate.add(candidateDetails);
                        }
                        relation1TuplePartOfCandidateList = true;
                    }
                    if (!relation1TuplePartOfCandidateList) {
                        CandidateDetailsNRAString tmp = new CandidateDetailsNRAString(tupleRIDRelation1.getRID(), null, new float[]{mergeAttributeValue1, 0}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, true, false, joinAttributeValue1);
                        addToTopKCandidateIndexList(topKCandidate.size(), tmp.getLowerBound(), tmp.getUpperBound());
                        topKCandidate.add(tmp);

                    }
                }
                if (tupleRIDRelation2 != null) {
                    Set<CandidateDetailsNRAString> duplicateCandidate2 = checkTupleWithTopKCandidate2(joinAttributeValue2, mergeAttributeValue2, tupleRIDRelation2.getRID());

                    if (duplicateCandidate2.size() > 0) {
                        for (CandidateDetailsNRAString candidateDetails : duplicateCandidate2) {
                            addToTopKCandidateIndexList(topKCandidate.size(), candidateDetails.getLowerBound(), candidateDetails.getUpperBound());
                            topKCandidate.add(candidateDetails);
                        }
                        // check this assignment is correct or not.
                        relation2TuplePartOfCandidateList = true;
                    }

                    if (!relation2TuplePartOfCandidateList) {
                        //DOn't use default value as 1000 for upper bound use value of first tuple if complementry relation. - solved.
                        CandidateDetailsNRAString tmp = new CandidateDetailsNRAString(null, tupleRIDRelation2.getRID(), new float[]{0, mergeAttributeValue2}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, false, true, joinAttributeValue2);
                        addToTopKCandidateIndexList(topKCandidate.size(), tmp.getLowerBound(), tmp.getUpperBound());
                        topKCandidate.add(tmp);
                    }
                }


                topKCandidateIndexList.sort((o1, o2) -> o2.get(0).compareTo(o1.get(0)));

                if (topKCandidateIndexList.size() > k) {
                    if (topKCandidateIndexList.get(k - 1).get(0) > mergeAttributeValue1 + mergeAttributeValue2 && !containsDuplicates) {
                        System.out.println("***************Found top K without iterating over entire relation.********************");
                        break;
                    }
                }
                tupleRIDRelation1 = relation1.get_next1();
                tupleRIDRelation2 = relation2.get_next1();

//                System.out.println("Number of elements in the candidate list : " + topKCandidate.size());
            }

            int counter = 1;
            for (ArrayList<Float> candidate : topKCandidateIndexList) {
                int index = candidate.get(2).intValue();
                Tuple t1 = new Tuple();
                try {
                    t1.setHdr((short) in1.length, in1, t1_str_sizes);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    e.printStackTrace();
                }
                if (topKCandidate.get(index).getRidRel1() != null) {
                    t1 = relation3.fetchRecord(topKCandidate.get(index).getRidRel1());
                }
                Tuple rid_tuple = new Tuple(t1.getTupleByteArray(), t1.getOffset(), t1.getLength());
                rid_tuple.setHdr((short) in1.length, in1, t1_str_sizes);
//                rid_tuple.print(in1);
//                System.out.println("Printed tuple for t1 : "  + rid_tuple.getIntFld(1) + " " + rid_tuple.getStrFld(2) + " " + rid_tuple.getIntFld(3) + " " + rid_tuple.getFloFld(4));
                Tuple t2 = new Tuple();
                try {
                    t2.setHdr((short) in2.length, in2, t2_str_sizes);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    e.printStackTrace();
                }
                if (topKCandidate.get(index).getRidRel2() != null) {
                    t2 = relation4.fetchRecord(topKCandidate.get(index).getRidRel2());

                }
                Tuple rid_tuple2 = new Tuple(t2.getTupleByteArray(), t2.getOffset(), t2.getLength());
                rid_tuple2.setHdr((short) in2.length, in2, t2_str_sizes);
//                rid_tuple2.print(in2);
//                System.out.println("Printed tuple for tuple 2  : "  + rid_tuple2.getIntFld(1) + " " + rid_tuple2.getIntFld(2) + " " + rid_tuple2.getStrFld(3));
                Tuple tuple = new Tuple();
                AttrType[] temp = new AttrType[len_in1 + len_in2];
                TupleUtils.setup_op_tuple(tuple, temp,
                        in1, len_in1, in2, len_in2,
                        t1_str_sizes, t2_str_sizes,
                        proj_list, (len_in1+ len_in2));
                Projection.Join(rid_tuple, in1,
                        rid_tuple2, in2,
                        tuple, proj_list, (len_in1+ len_in2));
                if (materialisedTable!= null){
                    materialisedTable.insertRecord(tuple.getTupleByteArray());
                } else {
                    tuple.print(temp);
                }
                if (counter == k) {
                    break;
                }
                counter++;
            }
        }
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

    private Set<CandidateDetailsNRAString> checkTupleWithTopKCandidate(String joinAttributeValue1, float mergeAttributeValue1, RID tuple1Rid) {
        int i = 0;
        Set<CandidateDetailsNRAString> tmp = new LinkedHashSet<>();
        for (CandidateDetailsNRAString candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier().equals(joinAttributeValue1)) {
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
                    CandidateDetailsNRAString tmp1 = new CandidateDetailsNRAString(tuple1Rid, candidateDetails.getRidRel2(),
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

    private Set<CandidateDetailsNRAString> checkTupleWithTopKCandidate2(String joinAttributeValue2, float mergeAttributeValue2, RID tuple2RID) {
        int i = 0;
        Set<CandidateDetailsNRAString> tmp = new LinkedHashSet<>();
        for (CandidateDetailsNRAString candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier().equals(joinAttributeValue2)) {
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
                    CandidateDetailsNRAString tmp1 = new CandidateDetailsNRAString(candidateDetails.getRidRel1(), tuple2RID,
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
