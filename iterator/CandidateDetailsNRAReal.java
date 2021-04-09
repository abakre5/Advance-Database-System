package iterator;

import global.RID;

public class CandidateDetailsNRAReal extends CandidateDetailsNRA{
    float identifier;

    public CandidateDetailsNRAReal() {
    }

    public CandidateDetailsNRAReal(RID ridRel1, RID ridRel2, float[] lowerBound, float[] upperBound, boolean relation1Seen, boolean relation2Seen, float identifier) {
        super(ridRel1, ridRel2, lowerBound, upperBound, relation1Seen, relation2Seen);
        this.identifier = identifier;
    }

    public float getIdentifier() {
        return identifier;
    }

    public void setIdentifier(float identifier) {
        this.identifier = identifier;
    }
}
