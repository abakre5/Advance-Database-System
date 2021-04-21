package hash;

public class StrKey extends KeyClass {
    private String key;

    public String toString() {
        return key;
    }

    /**
     * Class constructor
     *
     * @param value the value of the integer key to be set
     */
    public StrKey(String value) {
        key = value;
    }

    /**
     * get a copy of the float key
     *
     * @return the reference of the copy
     */
    public String getKey() {
        return key;
    }

    /**
     * set the float key value
     */
    public void setKey(String value) {
        key = value;
    }

    public int hashCode() {
        return key.hashCode();
    }
}
