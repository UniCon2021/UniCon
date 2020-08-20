package unicon.longRange.base;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import utils.LongPairWritable;

import java.io.IOException;

public class CCComputation extends Configured implements Tool {

    private Path input;
    private Path output;
    private final String title;

    public CCComputation(Path input, Path output) {
        this.input = input;
        this.output = output;
        this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
    }

    @Override
    public int run(String[] args) throws Exception {
        Job myJob = Job.getInstance(getConf(), title);
        myJob.setJarByClass(CCComputation.class);
        myJob.setMapperClass(CCMapper.class);
        myJob.setReducerClass(CCReducer.class);
        myJob.setMapOutputKeyClass(LongWritable.class);
        myJob.setMapOutputValueClass(LongPairWritable.class);
        myJob.setInputFormatClass(SequenceFileInputFormat.class);
        myJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(myJob, input);
        FileOutputFormat.setOutputPath(myJob, output);

        myJob.waitForCompletion(true);

        return 0;
    }

    public static class CCMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongPairWritable> {
        LongWritable outKey = new LongWritable();
        LongPairWritable outVal = new LongPairWritable();
        int numPartitions;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numPartitions = context.getConfiguration().getInt("numPartitions", 1);
        }

        @Override
        protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            long u = pure(key.get());
            long v = pure(value.get());

            if (u >= v) outKey.set(u % numPartitions);
            else outKey.set(v % numPartitions);
            outVal.set(u, v);
            context.write(outKey, outVal);
        }

        private long pure(long u) { return u < 0 ? ~u : u; }
    }

    public static class CCReducer extends Reducer<LongWritable, LongPairWritable, LongWritable, LongWritable> {
        LongWritable outKey = new LongWritable();
        LongWritable outVal = new LongWritable();
        long maxNode;
        int numPartitions;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            maxNode = context.getConfiguration().getLong("maxNode", -1);
            numPartitions = context.getConfiguration().getInt("numPartitions", -1);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
            Rem uf = new Rem(maxNode, key.get(), numPartitions);
            for (LongPairWritable val : values) {
                long u = val.i;
                long v = val.j;
                uf.union(u, v);
            }

            for (LongPairWritable pair : uf.p) {
                long u = pair.i;
                long up = uf.find(u);
                if (u != up) {
                    outKey.set(u);
                    outVal.set(up);
                    context.write(outKey, outVal);
                }
            }
        }
    }
}
