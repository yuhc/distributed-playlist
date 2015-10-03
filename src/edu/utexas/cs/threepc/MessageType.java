package edu.utexas.cs.threepc;

/**
 * Created by zhitingz on 10/3/15.
 */
public enum MessageType {
    Commit(1), Abort(2), VoteReq(3), Precommit(4);

    private int index;

    MessageType(int index) {
        this.index = index;
    }
}
