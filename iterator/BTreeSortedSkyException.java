package iterator;

import chainexception.*;

import java.lang.*;

public class BTreeSortedSkyException extends ChainException {
    public BTreeSortedSkyException(String s) {
        super(null, s);
    }

    public BTreeSortedSkyException(Exception prev, String s) {
        super(prev, s);
    }
}
