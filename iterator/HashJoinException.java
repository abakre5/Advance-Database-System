package iterator;

import chainexception.ChainException;

public class HashJoinException extends ChainException {
    public HashJoinException(Exception prev, String s) {
        super(prev, s);
    }
}
