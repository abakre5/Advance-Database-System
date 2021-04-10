package hash;

public class FloatKey extends KeyClass{
    private Float key;

    public String toString() {
        return key.toString();
    }

    /**
     * Class constructor
     *
     * @param value the value of the integer key to be set
     */
    public FloatKey(Float value) {
        key = value.floatValue();
    }

    /**
     * Class constructor
     *
     * @param value the value of the float key to be set
     */
    public FloatKey(float value) {
        key = value;
    }


    /**
     * get a copy of the float key
     *
     * @return the reference of the copy
     */
    public Float getKey() {
        return key.floatValue();
    }

    /**
     * set the float key value
     */
    public void setKey(Float value) {
        key = value.floatValue();
    }
}
