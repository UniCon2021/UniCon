package unicon.intRange.base;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.StringTokenizer;

public class UnionFindJobForTest extends Configured implements Tool {
    private int numNodes;
    private int[] parent;

    public UnionFindJobForTest(int numNodes){
        this.numNodes = numNodes + 1;
        parent = new int[numNodes + 1];
        for (int i = 0; i < numNodes + 1; i++) {
            parent[i] = i;
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        BufferedReader br = new BufferedReader(new FileReader(input));

        String line;
        while( (line = br.readLine()) != null ){
            StringTokenizer st = new StringTokenizer(line);
            int u = Integer.parseInt(st.nextToken());
            int v = Integer.parseInt(st.nextToken());

            union(u, v);
        }

        br.close();

        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        for(int i=0; i<numNodes; i++){
            int cc = find(i);
            if(i != cc) {
                bw.write(i + "\t" + cc + "\n");
            }
        }

        bw.close();

        return 0;
    }

    public void union(int x, int y) {
        int r1 = find(x);
        int r2 = find(y);

        if (r1 > r2) {
            parent[r1] = r2;
        }
        else if (r1 < r2) {
            parent[r2] = r1;
        }
    }

    public int find(int x) {
        while (parent[x] != x) {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    }
}
