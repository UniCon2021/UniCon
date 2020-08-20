package unicon.longRange.base;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.StringTokenizer;

public class UnionFindJobForTest extends Configured implements Tool {
    private long numNodes;
    Long2LongMap parent;

    public UnionFindJobForTest(long numNodes){
        this.numNodes = numNodes + 1;
        parent = new Long2LongOpenHashMap();
        parent.defaultReturnValue(-1);
    }

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        BufferedReader br = new BufferedReader(new FileReader(input));

        String line;
        while( (line = br.readLine()) != null ){
            StringTokenizer st = new StringTokenizer(line);
            long u = Long.parseLong(st.nextToken());
            long v = Long.parseLong(st.nextToken());

            union(u, v);
        }

        br.close();

        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        for(long i=0; i<numNodes; i++){
            if (i != parent.get(i)) {
                long cc = find(i);
                if(i != cc) {
                    bw.write(i + "\t" + cc + "\n");
                }
            }
        }

        bw.close();

        return 0;
    }

    public void union(long x, long y) {
        long r1 = find(x);
        long r2 = find(y);

        if (r1 > r2) {
            parent.put(r1, r2);
        }
        else if (r1 < r2) {
            parent.put(r2, r1);
        }
    }

    public long find(long x) {
        while (parent.get(x) != -1) {
            long ppx = parent.get(parent.get(x));
            if (ppx != -1) {
                parent.put(x, ppx);
            }
            x = parent.get(x);
        }
        return x;
    }
}
