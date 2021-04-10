package global;

/**
 * @author Abhishek Bakare
 */
public class AggType {
    public static final int aggMax = 0;
    public static final int aggMin = 1;
    public static final int aggAvg = 2;
    public static final int aggSkyline = 3;

    public int aggType;

    public AggType(int _aggType) {
        aggType = _aggType;
    }

    public String toString() {

        switch (aggType) {
            case aggMax:
                return "aggMax";
            case aggMin:
                return "aggMin";
            case aggAvg:
                return "aggAvg";
            case aggSkyline:
                return "aggSkyline";
        }
        return ("Unexpected AggType " + aggType);
    }
}
