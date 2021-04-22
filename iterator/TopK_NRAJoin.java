package iterator;

import bufmgr.PageNotReadException;
import catalog.AttrDesc;
import catalog.Catalogindexnotfound;
import global.AttrType;
import global.ExtendedSystemDefs;
import global.RID;
import heap.*;
import index.IndexScan;

import java.io.IOException;
import java.util.*;

import static global.AttrType.*;

/**
 * This class handles the logic to find the top K joined elements for given two relations using NRA Algorithm.
 * @author magraw12
 */
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
    Heapfile materialisedTable = null;
    String materialisedTableName = "";
    IndexScan outerIndexScan;
    IndexScan innerIndexScan;

    public TopK_NRAJoin(AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1, FldSpec mergeAttr1, AttrType[] in2, int len_in2, short[] t2_str_sizes, FldSpec joinAttr2, FldSpec mergeAttr2, String relationName1, String relationName2, int k, int n_pages, String materialisedTableName, IndexScan outerIndexScan, IndexScan innerIndexScan) throws IOException, PageNotReadException, WrongPermat, JoinsException, InvalidTypeException, TupleUtilsException, UnknowAttrType, FileScanException, PredEvalException, InvalidTupleSizeException, InvalidRelation, FieldNumberOutOfBoundException {
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

    /**
     * this methods comapres the tuples from both the relations with the candidate tuples to find the top k tuple.
     * @throws Exception
     */
    private void computeKJoin() throws Exception {
        IndexScan relation1 = outerIndexScan;
        FileScan relation3 = getFileScan(relationName1, (short) len_in1, in1, t1_str_sizes);
        IndexScan relation2 = innerIndexScan;
        FileScan relation4 = getFileScan(relationName2, (short) len_in2, in2, t2_str_sizes);
        float joinAttributeValue1 = 0;
        float joinAttributeValue2 = 0;
        float mergeAttributeValue1 = 0;
        float mergeAttributeValue2 = 0;

        FldSpec[] proj_list = new FldSpec[len_in1 + len_in2];
        for (int i = 1; i <= len_in1; i++) {
            proj_list[i - 1] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        for (int i = 1; i <= len_in2; i++) {
            proj_list[len_in1 + i - 1] = new FldSpec(new RelSpec(RelSpec.innerRel),  i);
        }

            TupleRIDPair tupleRIDRelation1 = relation1.get_next1();
            TupleRIDPair tupleRIDRelation2 = relation2.get_next1();

            while (tupleRIDRelation1 != null || tupleRIDRelation2 != null) {
                Tuple tupleRelation1;
                Tuple tupleRelation2;
                if (tupleRIDRelation1 != null) {
                    tupleRelation1 = tupleRIDRelation1.getTuple();
                    AttrType x = in1[joinAttr1.offset - 1];
                    switch (x.attrType) {
                        case attrInteger:
                            joinAttributeValue1 = (float) tupleRelation1.getIntFld(joinAttr1.offset);
                            break;
                        case attrReal:
                            joinAttributeValue1 = tupleRelation1.getFloFld(joinAttr1.offset);
                            break;
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
                    switch (x.attrType) {
                        case attrInteger:
                            joinAttributeValue2 = (float) tupleRelation2.getIntFld(joinAttr2.offset);
                            break;
                        case attrReal:
                            joinAttributeValue2 = tupleRelation2.getFloFld(joinAttr2.offset);
                            break;
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
                    Set<CandidateDetailsNRAReal> duplicateCandidate = checkTupleWithTopKCandidate(joinAttributeValue1, mergeAttributeValue1, tupleRIDRelation1.getRID());
                    if (duplicateCandidate.size() > 0) {
                        for (CandidateDetailsNRAReal candidateDetailsNRAReal : duplicateCandidate) {
                            addToTopKCandidateIndexList(topKCandidate.size(), candidateDetailsNRAReal.getLowerBound(), candidateDetailsNRAReal.getUpperBound());
                            topKCandidate.add(candidateDetailsNRAReal);
                        }
                        relation1TuplePartOfCandidateList = true;
                    }
                    if (!relation1TuplePartOfCandidateList) {
                        CandidateDetailsNRAReal tmp = new CandidateDetailsNRAReal(tupleRIDRelation1.getRID(), null, new float[]{mergeAttributeValue1, 0}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, true, false, joinAttributeValue1);
                        addToTopKCandidateIndexList(topKCandidate.size(), tmp.getLowerBound(), tmp.getUpperBound());
                        topKCandidate.add(tmp);

                    }
                }
                if (tupleRIDRelation2 != null) {
                    Set<CandidateDetailsNRAReal> duplicateCandidate2 = checkTupleWithTopKCandidate2(joinAttributeValue2, mergeAttributeValue2, tupleRIDRelation2.getRID());

                    if (duplicateCandidate2.size() > 0) {
                        for (CandidateDetailsNRAReal candidateDetailsNRAReal : duplicateCandidate2) {
                            addToTopKCandidateIndexList(topKCandidate.size(), candidateDetailsNRAReal.getLowerBound(), candidateDetailsNRAReal.getUpperBound());
                            topKCandidate.add(candidateDetailsNRAReal);
                        }
                        relation2TuplePartOfCandidateList = true;
                    }

                    if (!relation2TuplePartOfCandidateList) {
                        CandidateDetailsNRAReal tmp = new CandidateDetailsNRAReal(null, tupleRIDRelation2.getRID(), new float[]{0, mergeAttributeValue2}, new float[]{mergeAttributeValue1, mergeAttributeValue2}, false, true, joinAttributeValue2);
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

        relation1.close();
        relation2.close();
    }

    /**
     * update the candidate index list.
     * @param index - index of candidate in the candidate list
     * @param lowerBound - lower bound of candidate.
     * @param upperBound - upper bound of candidate.
     */
    public void updateTopKCandidateIndexList(int index, float[] lowerBound, float[] upperBound) {
        for (ArrayList<Float> itr : topKCandidateIndexList) {
            if (itr.get(2) == index) {
                itr.set(0, lowerBound[0] + lowerBound[1]);
                itr.set(1, upperBound[0] + upperBound[1]);
            }
        }
    }

    /**
     * Adds given tuple to candidate list.
     * @param index - index of candidate in the candidate list
     * @param lowerBound - lower bound of candidate.
     * @param upperBound - upper bound of candidate.
     */
    public void addToTopKCandidateIndexList(int index, float[] lowerBound, float[] upperBound) {
        ArrayList<Float> tmp = new ArrayList<>();
        tmp.add(lowerBound[0] + lowerBound[1]);
        tmp.add(upperBound[0] + upperBound[1]);
        tmp.add((float) index);
        topKCandidateIndexList.add(tmp);
    }

    /**
     * Method compares the tuple in relation 1 with join candidates in the candidate list.
     * @param joinAttributeValue1 join attribute value.
     * @param mergeAttributeValue1  merge attribute value.
     * @param tuple1Rid RID given tuple.
     * @return list of new candidates.
     */
    private Set<CandidateDetailsNRAReal> checkTupleWithTopKCandidate(float joinAttributeValue1, float mergeAttributeValue1, RID tuple1Rid) {
        int i = 0;
        Set<CandidateDetailsNRAReal> tmp = new LinkedHashSet<>();
        for (CandidateDetailsNRAReal candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier() == joinAttributeValue1) {
                if (!candidateDetails.isRelation1Seen()) {
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

    /**
     * method compares the tuple in relation 2 with join candidates in the candidate list.
     * @param joinAttributeValue2 - join attribute value.
     * @param mergeAttributeValue2 - merge attribute value.
     * @param tuple2RID - RID of the given tuple.
     * @return - list of new candidates.
     */
    private Set<CandidateDetailsNRAReal> checkTupleWithTopKCandidate2(float joinAttributeValue2, float mergeAttributeValue2, RID tuple2RID) {
        int i = 0;
        Set<CandidateDetailsNRAReal> tmp = new LinkedHashSet<>();
        for (CandidateDetailsNRAReal candidateDetails : topKCandidate) {
            boolean candidateFlagToUpdateTheBounds = true;
            if (candidateDetails.getIdentifier() == joinAttributeValue2) {
                if (!candidateDetails.isRelation2Seen()) {
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
