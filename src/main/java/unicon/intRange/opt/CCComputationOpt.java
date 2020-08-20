package unicon.intRange.opt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import utils.IntPairWritable;

import java.io.IOException;

public class CCComputationOpt extends Configured implements Tool {

    private Path input;
    private Path output;
    private final String title;
    private int numRounds;

    public CCComputationOpt(Path input, Path output, int numRounds) {
        this.input = input;
        this.output = output;
        this.numRounds = numRounds;
        this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), title);
        job.setJarByClass(CCComputationOpt.class);
        job.setMapperClass(CCMapper.class);
        job.setReducerClass(CCReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntPairWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(getConf());
        for (int i = 0; i <= numRounds; i++) {
            if (fs.exists(input.suffix("." + i + "/sep")))
                FileInputFormat.addInputPath(job, input.suffix("." + i + "/sep"));
        }
        
        if (fs.exists(input.suffix("." + numRounds + "/out"))) {
            FileInputFormat.addInputPath(job, input.suffix("." + numRounds + "/out"));
        }

        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

        return 0;
    }

    public static class CCMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntPairWritable> {

        IntWritable outKey = new IntWritable();
        IntPairWritable outVal = new IntPairWritable();
        int numPartitions;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numPartitions = context.getConfiguration().getInt("numPartitions", 1);
        }

        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            int u = pure(key.get());
            int v = pure(value.get());

            if (u >= v) outKey.set(u % numPartitions);
            else outKey.set(v % numPartitions);

            outVal.set(u, v);

            context.write(outKey, outVal);
        }

        private int pure(int u) { return u < 0 ? ~u : u; }
    }

    public static class CCReducer extends Reducer<IntWritable, IntPairWritable, IntWritable, IntWritable> {

        IntWritable outKey = new IntWritable();
        IntWritable outVal = new IntWritable();
        int maxNode;
        int numPartitions;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            maxNode = context.getConfiguration().getInt("maxNode", -1);
            numPartitions = context.getConfiguration().getInt("numPartitions", -1);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
            Rem uf = new Rem(maxNode, key.get(), numPartitions);
            for (IntPairWritable val : values) {
                int u = val.u;
                int v = val.v;
                uf.union(u, v);
            }

            for (IntPairWritable pair : uf.p) {
                int u = pair.u;
                int up = uf.find(u);
                if (u != up) {
                    outKey.set(u);
                    outVal.set(up);
                    context.write(outKey, outVal);
                }
            }
        }
    }

}
