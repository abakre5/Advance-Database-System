package iterator;

import global.RID;

import java.util.Arrays;

public class CandidateDetailsNRA {
    private RID ridRel1;
    private RID ridRel2;
    private float[] lowerBound = new float[2];
    private float[] upperBound = new float[2];
    private boolean relation1Seen = false;
    private boolean relation2Seen = false;

    public CandidateDetailsNRA() {
        //default Constructor
    }

    public CandidateDetailsNRA(RID ridRel1, RID ridRel2, float[] lowerBound, float[] upperBound, boolean relation1Seen, boolean relation2Seen) {
        this.ridRel1 = ridRel1;
        this.ridRel2 = ridRel2;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.relation1Seen = relation1Seen;
        this.relation2Seen = relation2Seen;
    }

    public RID getRidRel2() {
        return ridRel2;
    }

    public void setRidRel2(RID ridRel2) {
        this.ridRel2 = ridRel2;
    }

    public float[] getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(float[] lowerBound) {
        this.lowerBound = lowerBound;
    }

    public void updateLowerBound(int index, float lowerBound) {
        this.lowerBound[index] = lowerBound;
    }

    public RID getRidRel1() {
        return ridRel1;
    }

    public void setRidRel1(RID ridRel1) {
        this.ridRel1 = ridRel1;
    }

    public float[] getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(float[] upperBound) {
        this.upperBound = upperBound;
    }

    public void updateUpperBound(int index, float upperBound) {
        this.upperBound[index] = upperBound;
    }

    public boolean isRelation1Seen() {
        return relation1Seen;
    }

    public void setRelation1Seen(boolean relation1Seen) {
        this.relation1Seen = relation1Seen;
    }

    public boolean isRelation2Seen() {
        return relation2Seen;
    }

    public void setRelation2Seen(boolean relation2Seen) {
        this.relation2Seen = relation2Seen;
    }

    @Override
    public String toString() {
        return "CandidateDetailsNRA{" +
                "ridRel1=" + ridRel1 +
                ", ridRel2=" + ridRel2 +
                ", lowerBound=" + Arrays.toString(lowerBound) +
                ", upperBound=" + Arrays.toString(upperBound) +
                ", relation1Seen=" + relation1Seen +
                ", relation2Seen=" + relation2Seen +
                '}';
    }

}
