package utils;

public class Pair<L, R> {
    final L u;
    final R v;

    public Pair(L u, R v) {
        this.u = u;
        this.v = v;
    }

    public L getU() {
        return u;
    }

    public R getV() {
        return v;
    }

    static <L, R> Pair<L, R> of(L u, R v) {
        return new Pair<L, R>(u, v);
    }
}
