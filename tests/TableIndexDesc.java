package tests;

import global.IndexType;

public class TableIndexDesc {
    private final int type;
    private final int attributeIndex;

    public TableIndexDesc(int type, int attributeIndex) {
        this.type = type;
        this.attributeIndex = attributeIndex;
    }

    public int getType() {
        return type;
    }

    public int getAttributeIndex() {
        return attributeIndex;
    }
}
