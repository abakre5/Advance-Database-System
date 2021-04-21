package hash;

public class IntKey extends KeyClass {
    private int key;

    public String toString() {
        return "" + key;
    }

    /**
     * Class constructor
     *
     * @param value the value of the integer key to be set
     */
    public IntKey(int value) {
        key = value;
    }

    /**
     * get a copy of the float key
     *
     * @return the reference of the copy
     */
    public int getKey() {
        return key;
    }

    /**
     * set the float key value
     */
    public void setKey(int value) {
        key = value;
    }


    public int hashCode() {
        return this.key;
    }
}
