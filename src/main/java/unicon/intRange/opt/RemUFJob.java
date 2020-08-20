package unicon.intRange.opt;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class RemUFJob extends Configured implements Tool {

    Int2IntMap parent;
    private final Path input;
    private final Path output;
    public long outputSize = 0;
    private int maxNode;

    public RemUFJob(Path input, Path output, int maxNode) {
        this.input = input;
        this.output = output;
        this.maxNode = maxNode;
        parent = new Int2IntOpenHashMap();
        parent.defaultReturnValue(-1);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        fs.delete(output, true);

        IntWritable iKey = new IntWritable();
        IntWritable iValue = new IntWritable();

        FileStatus[] status;
        
        if(fs.getFileStatus(input).isDirectory()){
            status = fs.listStatus(input, path -> path.getName().startsWith("part"));
        }
        else{
            status = new FileStatus[1];
            status[0] = fs.getFileStatus(input);
        }

        for (FileStatus statu : status) {
            try {
                SequenceFile.Reader sr = new SequenceFile.Reader(fs, statu.getPath(), conf);

                while (sr.next(iKey, iValue)) {
                    int u = iKey.get() >= 0 ? iKey.get() : ~iKey.get();
                    int v = iValue.get() >= 0 ? iValue.get() : ~iValue.get();

                    if (find(u) != find(v)) { union(u, v); }
                }
                sr.close();

            } catch (Exception ignored) {}
        }

        final SequenceFile.Writer out = new SequenceFile.Writer(fs, conf, output, IntWritable.class, IntWritable.class);

        final IntWritable ou = new IntWritable();
        final IntWritable ov = new IntWritable();

        for (int u = 0; u < maxNode; u++) {
            try {
                int v = find(u);
                if (u != v) {
                    ou.set(u);
                    ov.set(v);
                    out.append(ou, ov);
                    outputSize++;
                }
            } catch (IOException ignored) { }
        }

        out.close();

        return 0;
    }

    public void union(int x, int y) {
        int r1 = find(x);
        int r2 = find(y);

        if (r1 > r2) parent.put(r1, r2);
        else if (r1 < r2) parent.put(r2, r1);
    }

    public int find(int x) {
        while (parent.get(x) != -1) {
            int ppx = parent.get(parent.get(x));
            if (ppx != -1) {
                parent.put(x, ppx);
            }
            x = parent.get(x);
        }
        return x;
    }
}