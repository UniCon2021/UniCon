package unicon.intRange.base;

public class Rem {
    HybridInt2IntMap p;

    public Rem(int numNodes, int pid, int numParts){
        p = new HybridInt2IntMap(numNodes, pid, numParts);
    }

    /**
     *
     * @param u
     * @param v
     * @return `true` if two different components are united,
     *         `false` if `u` and `v` are already in the same component.
     */
    public boolean union(int u, int v){
        int up, vp;
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

    public int find(int u){
        int up;
        while(u != (up = p.get(u))){
            p.put(u, p.get(up));
            u = up;
        }
        return u;
    }

}
