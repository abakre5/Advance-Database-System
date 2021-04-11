package iterator;

import chainexception.ChainException;

public class IndexNestedLoopException extends ChainException {
    public IndexNestedLoopException(String s) {
        super(null, s);
    }

    public IndexNestedLoopException(Exception prev, String s) {
        super(prev, s);
    }
}