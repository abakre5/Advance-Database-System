package iterator;

import global.RID;

public class CandidateDetailsNRAInt extends CandidateDetailsNRA {
    private int identifier;

    public CandidateDetailsNRAInt() {
    }

    public CandidateDetailsNRAInt(RID ridRel1, RID ridRel2, float[] lowerBound, float[] upperBound, boolean relation1Seen, boolean relation2Seen, int identifier) {
        super(ridRel1, ridRel2, lowerBound, upperBound, relation1Seen, relation2Seen);
        this.identifier = identifier;
    }

    public int getIdentifier() {
        return identifier;
    }

    public void setIdentifier(int identifier) {
        this.identifier = identifier;
    }
}
