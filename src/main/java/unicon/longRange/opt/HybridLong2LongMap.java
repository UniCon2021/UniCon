package unicon.longRange.opt;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import utils.LongPairWritable;

import java.util.Iterator;

public class HybridLong2LongMap implements Iterable<LongPairWritable> {

    long pid;
    int numParts;
    long[] local;
    Long2LongOpenHashMap global;
    /**
     *
     * @param numNodes The number of global nodes
     * @param pid The id of the current partition
     * @param numParts The number of partitions
     */
    public HybridLong2LongMap(long numNodes, long pid, int numParts){
        this.pid = pid;
        this.numParts = numParts;
        local = new long[(int) ((numNodes / numParts)+1)];
        for(long i=0, j=pid; i<local.length; i++, j+=numParts)
            local[(int) i] = j;

        global = new Long2LongOpenHashMap();
        global.defaultReturnValue(-1);
    }

    public void put(long a, long b){
        if (a % numParts == pid)
            local[(int) (a / numParts)] = b;
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


    public long get(long a) {
        return a % numParts == pid ? local[(int) (a/numParts)] : global.getOrDefault(a, a);
    }

    public int getMaxSize(){
        return local.length + global.size();
    }
    @Override
    public Iterator<LongPairWritable> iterator() {
        return new Iterator<LongPairWritable>() {
            LongPairWritable x = new LongPairWritable();
            int pos = 0;
            long n = pid;
            ObjectIterator<Long2LongMap.Entry> git = global.long2LongEntrySet().fastIterator();

            @Override
            public boolean hasNext() {
                return pos < local.length || git.hasNext();
            }

            @Override
            public LongPairWritable next() {
                if(pos < local.length){
                    x.set(n, local[pos]);
                    n += numParts;
                    pos += 1;
                }
                else{
                    Long2LongMap.Entry t = git.next();
                    x.set(t.getLongKey(), t.getLongValue());
                }
                return x;
            }
        };
    }
}
