package unicon.longRange.base;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import utils.LongPairWritable;
import utils.Pair;

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
        myJob.setMapOutputKeyClass(LongWritable.class);
        myJob.setMapOutputValueClass(LongPairWritable.class);
        myJob.setOutputKeyClass(LongWritable.class);
        myJob.setOutputValueClass(LongWritable.class);
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

    public static class UniStarMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongPairWritable> {

        LongWritable ok = new LongWritable();
        LongPairWritable ov = new LongPairWritable();
        int numParts;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", -1);
        }

        @Override
        protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            long u = key.get();
            long v = value.get();
            boolean utag = u < 0;
            boolean vtag = v < 0;
            ov.set(pure(u), pure(v));
            int up = (int) (ov.i % numParts);
            int vp = (int) (ov.j % numParts);
            if (!utag) {
                ok.set(up);
                context.write(ok, ov);
            }
            if (!vtag && (utag || up != vp)) {
                ok.set(vp);
                context.write(ok, ov);
            }
        }

        private long pure(long u) {
            return u < 0 ? ~u : u;
        }
    }

    public static class UniStarReducer extends Reducer<LongWritable, LongPairWritable, LongWritable, LongWritable> {

        LongWritable ok = new LongWritable();
        LongWritable ov = new LongWritable();
        int numParts;
        long numNodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", -1);
            numNodes = context.getConfiguration().getLong("numNodes", -1);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
            long pid = key.get();

            RemCUF uf = new RemCUF(numNodes, pid, numParts);
            long numChanges = 0;

            // union
            for (LongPairWritable val : values) {
                uf.union(val.i, val.j);
            }

            // refine & emit: need to be optimized
            ArrayList<Pair<Long, Long>> allPairs = new ArrayList<>();
            for (LongPairWritable pair : uf.p) {
                long u = pair.i;
                long up = uf.find(pair.i);

                if (u != up) allPairs.add(new Pair<>(up, u));
            }
            allPairs.sort((p1, p2) -> {
                long p1_u = p1.getU();
                long p1_v = p1.getV();
                long p2_u = p2.getU();
                long p2_v = p2.getV();

                if (p1_u != p2_u) return Long.compare(p1_u, p2_u);
                else return Long.compare(p1_v, p2_v);
            });

            long cc = -1;
            int cc_part = -1;
            long[] heads = new long[numParts];
            for (Pair pair : allPairs) {
                long u = (long) pair.getU();
                long v = (long) pair.getV();

                if (u != cc) {
                    cc = u;
                    cc_part = (int) (cc % numParts);
                    Arrays.fill(heads, -1);
                    heads[(int) (u % numParts)] = u;
                }

                int v_part = (int) (v % numParts);
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