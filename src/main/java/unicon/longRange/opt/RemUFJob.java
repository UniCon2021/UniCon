package unicon.longRange.opt;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class RemUFJob extends Configured implements Tool {

    Long2LongMap parent;
    private final Path input;
    private final Path output;
    public long outputSize = 0;
    private long maxNode;

    public RemUFJob(Path input, Path output, long maxNode) {
        this.input = input;
        this.output = output;
        this.maxNode = maxNode;
        parent = new Long2LongOpenHashMap();
        parent.defaultReturnValue(-1);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        fs.delete(output, true);

        LongWritable iKey = new LongWritable();
        LongWritable iValue = new LongWritable();

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
                    long u = iKey.get() >= 0 ? iKey.get() : ~iKey.get();
                    long v = iValue.get() >= 0 ? iValue.get() : ~iValue.get();

                    if (find(u) != find(v)) { union(u, v); }
                }
                sr.close();

            } catch (Exception ignored) {}
        }

        final SequenceFile.Writer out = new SequenceFile.Writer(fs, conf, output, LongWritable.class, LongWritable.class);

        final LongWritable ou = new LongWritable();
        final LongWritable ov = new LongWritable();

        for (long u = 0; u < maxNode; u++) {
            try {
                long v = find(u);
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

    public void union(long x, long y) {
        long r1 = find(x);
        long r2 = find(y);

        if (r1 > r2) parent.put(r1, r2);
        else if (r1 < r2) parent.put(r2, r1);
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