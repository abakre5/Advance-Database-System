package iterator;

import global.RID;

import java.util.Arrays;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CandidateDetailsNRAString)) return false;
        CandidateDetailsNRAString that = (CandidateDetailsNRAString) o;
        return (getIdentifier().equals(that.getIdentifier()) && isRelation1Seen() == that.isRelation1Seen() && isRelation2Seen() == that.isRelation2Seen() && getRidRel1().equals(that.getRidRel1()) && getRidRel2().equals(that.getRidRel2()) && Arrays.equals(getLowerBound(), that.getLowerBound()) && Arrays.equals(getUpperBound(), that.getUpperBound()));
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getRidRel1(), getRidRel2(), isRelation1Seen(), isRelation2Seen());
        result = 31 * result + Arrays.hashCode(getLowerBound());
        result = 31 * result + Arrays.hashCode(getUpperBound());
        return result;
    }
}
