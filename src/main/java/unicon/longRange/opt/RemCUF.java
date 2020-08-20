package unicon.longRange.opt;

public class RemCUF {

    HybridLong2LongMap p;

    public RemCUF(long numNodes, long pid, int numParts){
        p = new HybridLong2LongMap(numNodes, pid, numParts);
    }

    /**
     *
     * @param u
     * @param v
     * @return `true` if two different components are united,
     *         `false` if `u` and `v` are already in the same component.
     */
    public boolean union(long u, long v){
        long uc = u, vc = v;
        long up, vp;
        while((up = pure(p.get(uc))) != (vp = pure(p.get(vc)))){
            if(up < vp){
                p.put(vc, ~up);
                if(vp == vc){
                    // cancel tagging if the edge is not changed
                    if(vc == v && up == u) p.put(vc, up);
                    return true;
                }
                vc = vp;
            }
            else{
                p.put(uc, ~vp);
                if(up == uc){
                    // cancel tagging if the edge is not changed
                    if(uc == u && vp == v) p.put(uc, vp);
                    return true;
                }
                uc = up;
            }
        }

        return false;
    }

    public long find(long u){
        long up;
        while(u != (up = pure(p.get(u)))){
            long upp = pure(p.get(up));
            if(up != upp) p.put(u, ~upp);
            u = up;
        }
        return u;
    }

    public long pure(long u){
        return u < 0 ? ~u : u;
    }
    public boolean tag(long u){
        return p.get(u) < 0;
    }
}
