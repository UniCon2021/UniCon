package unicon.intRange.base;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import utils.IntPairWritable;

import java.util.Iterator;

public class HybridInt2IntMap implements Iterable<IntPairWritable> {

    int pid, numParts;
    int[] local;
    Int2IntOpenHashMap global;
    /**
     *
     * @param numNodes The number of global nodes
     * @param pid The id of the current partition
     * @param numParts The number of partitions
     */
    public HybridInt2IntMap(int numNodes, int pid, int numParts){
        this.pid = pid;
        this.numParts = numParts;
        local = new int[(numNodes / numParts)+1];
        for(int i=0, j=pid; i<local.length; i++, j+=numParts)
            local[i] = j;

        global = new Int2IntOpenHashMap();
        global.defaultReturnValue(-1);
    }

    public void put(int a, int b){
        if (a % numParts == pid)
            local[a / numParts] = b;
        else
            global.put(a, b);
    }

    public void putIfAbsent(int a, int b){
        if(a % numParts == pid) {
            int idx = a / numParts;
            if (local[idx] == -1) local[idx] = b;
        }
        else global.putIfAbsent(a, b);
    }

    public int get(int a) {
        return a % numParts == pid ? local[a/numParts] : global.getOrDefault(a, a);
    }

    public int getMaxSize(){
        return local.length + global.size();
    }
    @Override
    public Iterator<IntPairWritable> iterator() {
        return new Iterator<IntPairWritable>() {
            IntPairWritable x = new IntPairWritable();
            int pos = 0;
            int n = pid;
            ObjectIterator<Int2IntMap.Entry> git = global.int2IntEntrySet().fastIterator();
            @Override
            public boolean hasNext() {
                return pos < local.length || git.hasNext();
            }

            @Override
            public IntPairWritable next() {
                if(pos < local.length){
                    x.set(n, local[pos]);
                    n += numParts;
                    pos += 1;
                }
                else{
                    Int2IntMap.Entry t = git.next();
                    x.set(t.getIntKey(), t.getIntValue());
                }
                return x;
            }
        };
    }
}
