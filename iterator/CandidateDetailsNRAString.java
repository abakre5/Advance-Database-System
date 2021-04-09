package iterator;

import global.RID;

public class CandidateDetailsNRAString extends CandidateDetailsNRA {

    String identifier;

    public CandidateDetailsNRAString() {
    }

    public CandidateDetailsNRAString(RID ridRel1, RID ridRel2, float[] lowerBound, float[] upperBound, boolean relation1Seen, boolean relation2Seen, String identifier) {
        super(ridRel1, ridRel2, lowerBound, upperBound, relation1Seen, relation2Seen);
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
}
