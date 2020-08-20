package utils;

public class LongPair implements Comparable<LongPair> {
	public final long u, v;

	public LongPair(long u, long v) {
		this.u = u;
		this.v = v;
	}

	@Override
	public int compareTo(LongPair o) {
		if (u != o.u)
			return Long.compare(u, o.u);
		else
			return Long.compare(v, o.v);
	}
}
