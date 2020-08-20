package unicon.intRange.base;

import unicon.intRange.base.HybridInt2IntMap;

public class RemCUF {

    HybridInt2IntMap p;

    public RemCUF(int numNodes, int pid, int numParts){
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
        int uc = u, vc = v;
        int up, vp;
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

    public int find(int u){
        int up;
        while(u != (up = pure(p.get(u)))){
            int upp = pure(p.get(up));
            if(up != upp) p.put(u, ~upp);
            u = up;
        }
        return u;
    }

    public int pure(int u){
        return u < 0 ? ~u : u;
    }
    public boolean tag(int u){
        return p.get(u) < 0;
    }
}
