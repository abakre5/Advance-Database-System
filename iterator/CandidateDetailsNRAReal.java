package iterator;

import global.RID;

import java.util.Arrays;
import java.util.Objects;

public class CandidateDetailsNRAReal extends CandidateDetailsNRA{
    float identifier;

    public CandidateDetailsNRAReal(float identifier) {
        this.identifier = identifier;
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

    @Override
    public String toString() {
        return "CandidateDetailsNRAReal{" +
                "identifier=" + identifier +
                "} " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CandidateDetailsNRAReal)) return false;
        CandidateDetailsNRAReal that = (CandidateDetailsNRAReal) o;
        return Float.compare(that.getIdentifier(), getIdentifier()) == 0 && isRelation1Seen() == that.isRelation1Seen() && isRelation2Seen() == that.isRelation2Seen() && getRidRel1().equals(that.getRidRel1()) && getRidRel2().equals(that.getRidRel2()) && Arrays.equals(getLowerBound(), that.getLowerBound()) && Arrays.equals(getUpperBound(), that.getUpperBound());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getRidRel1(), getRidRel2(), isRelation1Seen(), isRelation2Seen());
        result = 31 * result + Arrays.hashCode(getLowerBound());
        result = 31 * result + Arrays.hashCode(getUpperBound());
        return result;
    }
}
