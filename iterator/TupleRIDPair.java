package iterator;

import global.RID;
import heap.Tuple;

public class TupleRIDPair {
    private Tuple tuple;
    private RID rid;
    public TupleRIDPair(Tuple tuple, RID rid) {
        this.tuple = tuple;
        this.rid = rid;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public RID getRID() {
        return rid;
    }
}
