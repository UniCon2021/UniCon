package unicon.longRange.base;

public class Rem {
    HybridLong2LongMap p;

    public Rem(long numNodes, long pid, int numParts){
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
        long up, vp;
        while((up = p.get(u)) != (vp = p.get(v))){
            if(up < vp){
                if(vp == v){
                    p.put(v, up);
                    return true;
                }
                p.put(v, up);
                v = vp;
            }
            else{
                if(up == u){
                    p.put(u, vp);
                    return true;
                }
                p.put(u, vp);
                u = up;
            }
        }
        return false;
    }

    public long find(long u){
        long up;
        while(u != (up = p.get(u))){
            p.put(u, p.get(up));
            u = up;
        }
        return u;
    }
}
