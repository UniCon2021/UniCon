package unicon.intRange.base;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import utils.Counters;
import utils.IntPairWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class UniStar extends Configured implements Tool {

    private Path input;
    private Path output;
    private final String title;
    long numChanges;
    public long reducerInputSize;
    public long totalOutputSize;

    public UniStar(Path input, Path output) {
        this.input = input;
        this.output = output;
        this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job myJob = Job.getInstance(getConf(), title);
        myJob.setJarByClass(UniStar.class);
        myJob.setMapperClass(UniStarMapper.class);
        myJob.setReducerClass(UniStarReducer.class);
        myJob.setMapOutputKeyClass(IntWritable.class);
        myJob.setMapOutputValueClass(IntPairWritable.class);
        myJob.setOutputKeyClass(IntWritable.class);
        myJob.setOutputValueClass(IntWritable.class);
        myJob.setInputFormatClass(SequenceFileInputFormat.class);
        myJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(myJob, input);
        FileOutputFormat.setOutputPath(myJob, output);
        myJob.waitForCompletion(true);

        this.numChanges = myJob.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
        reducerInputSize = myJob.getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
        totalOutputSize = myJob.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
        return 0;
    }

    public static class UniStarMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntPairWritable> {

        IntWritable ok = new IntWritable();
        IntPairWritable ov = new IntPairWritable();
        int numParts;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", -1);
        }

        private int pure(int u) {
            return u < 0 ? ~u : u;
        }

        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            int u = key.get();
            int v = value.get();
            boolean utag = u < 0;
            boolean vtag = v < 0;
            ov.set(pure(u), pure(v));
            int up = ov.u % numParts;
            int vp = ov.v % numParts;
            if (!utag) {
                ok.set(up);
                context.write(ok, ov);
            }
            if (!vtag && (utag || up != vp)) {
                ok.set(vp);
                context.write(ok, ov);
            }
        }
    }

    public static class UniStarReducer extends Reducer<IntWritable, IntPairWritable, IntWritable, IntWritable> {

        IntWritable ok = new IntWritable();
        IntWritable ov = new IntWritable();
        int numParts;
        int numNodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", -1);
            numNodes = context.getConfiguration().getInt("numNodes", -1);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
            int pid = key.get();

            RemCUF uf = new RemCUF(numNodes, pid, numParts);
            long numChanges = 0;

            // union
            for (IntPairWritable val : values) {
                uf.union(val.u, val.v);
            }

            // refine & emit: need to be optimized
            ArrayList<Long> allPairs = new ArrayList<>();
            for (IntPairWritable pair : uf.p) {
                int u = pair.u;
                int up = uf.find(u);
                if (u != up) allPairs.add(((long) up) << 32 | u);
            }
            allPairs.sort(null);

            int cc = -1;
            int cc_part = -1;
            int[] heads = new int[numParts];
            for (long pair : allPairs) {
                int u = (int) (pair >> 32);
                int v = (int) pair;

                if (u != cc) {
                    cc = u;
                    cc_part = cc % numParts;
                    Arrays.fill(heads, -1);
                    heads[u % numParts] = u;
                }

                int v_part = v % numParts;
                ok.set(v);
                if (heads[v_part] == -1) {
                    heads[v_part] = v;
                    if (!uf.tag(v)) {
                        if (v_part == pid) {
                            ov.set(cc_part == pid ? cc : ~cc);
                            context.write(ok, ov);
                        } else {
                            ok.set(~v);
                            ov.set(cc);
                            context.write(ok, ov);
                        }
                    } else {
                        numChanges++;
                        ov.set(cc);
                        context.write(ok, ov);
                    }
                } else {
                    if (v_part != pid) numChanges++;
                    ov.set(heads[v_part]);
                    context.write(ok, ov);
                }

            }
            context.getCounter(Counters.NUM_CHANGES).increment(numChanges);
        }
    }
}
