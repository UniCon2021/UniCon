package unicon.longRange.opt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import utils.Counters;
import utils.LongPair;
import utils.LongPairWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

public class UniStarOpt extends Configured implements Tool {

    private Path input;
    private Path output;
    private final String title;
    public long numChanges, numEdges, numInEdges, numCcEdges, numRemovedEdges;

    public UniStarOpt(Path input, Path output) {
        this.input = input;
        this.output = output;
        this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), title);
        job.setJarByClass(UniStarOpt.class);
        job.setMapperClass(UniStarMapper.class);
        job.setReducerClass(UniStarReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongPairWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
        
        this.numChanges = job.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
        this.numEdges = job.getCounters().findCounter(Counters.NUM_EDGES).getValue();
        this.numInEdges = job.getCounters().findCounter(Counters.IN_NUM_EDGES).getValue();
        this.numCcEdges = job.getCounters().findCounter(Counters.CC_NUM_EDGES).getValue();
        this.numRemovedEdges = job.getCounters().findCounter(Counters.REMOVED_EDGES).getValue();
        return 0;
    }

    public static class UniStarMapper extends Mapper<LongWritable, LongWritable, IntWritable, LongPairWritable> {

        IntWritable ok = new IntWritable();
        LongPairWritable ov = new LongPairWritable();
        int numParts;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", 0);
        }

        @Override
        protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            long u = key.get();
            long v = value.get();
            ov.set(u, v);
            boolean utag = u < 0;
            boolean vtag = v < 0;
            int up = (int) (pure(u) % numParts);
            int vp = (int) (pure(v) % numParts);
            
            if(!utag) {
            	ok.set(up);
            	context.write(ok, ov);
            }
            if(!vtag && (utag || up != vp)) {
            	ok.set(vp);
            	context.write(ok, ov);
            }
        }

        private long pure(long u) {
            return u < 0 ? ~u : u;
        }
    }

    public static class UniStarReducer extends Reducer<IntWritable, LongPairWritable, LongWritable, LongWritable> {

        LongWritable ok = new LongWritable();
        LongWritable ov = new LongWritable();
        int numParts;
        long numNodes;
        private MultipleOutputs<LongWritable, LongWritable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", 0);
            numNodes = context.getConfiguration().getLong("numNodes", 0);
            mos = new MultipleOutputs<>(context);
        }

        @Override
		protected void cleanup(Reducer<IntWritable, LongPairWritable, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}

		@Override
        protected void reduce(IntWritable key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
            long pid = key.get();
            RemCUF uf = new RemCUF(numNodes, pid, numParts);
            long numChanges = 0;
            long numEdges = 0;
            long numInEdges = 0;
            long numCcEdges = 0;
            long numRemovedEdges= 0;
            
            BitSet hasChild = new BitSet();
            BitSet hasPureNeighbor = new BitSet(); 
            
            // union
            // assume that pure(val.u) > pure(val.v)
            for (LongPairWritable val : values) {
            	if(pure(val.j) % numParts == pid) {
            		int v_local = (int) (pure(val.j) / numParts);
            		if(val.i >= 0) hasPureNeighbor.set(v_local);
            		hasChild.set(v_local);
            	}
            	if(pure(val.i) % numParts == pid) {
            		int u_local = (int) (pure(val.i) / numParts);
            		if(val.j >= 0) hasPureNeighbor.set(u_local);
            	}
                uf.union(pure(val.i), pure(val.j));
            }

            // refine & emit: need to be optimized
            ArrayList<LongPair> allPairs = new ArrayList<>();
            
            for (LongPairWritable pair : uf.p) {
                long u = pair.i;
                long v = uf.find(u);

                if (u == v) continue;

                int u_local = (int) (u / numParts);

                if (u % numParts == pid && !hasPureNeighbor.get(u_local) && !hasChild.get(u_local)) {
                    numRemovedEdges++;
                }
                else if (v % numParts == pid && !hasPureNeighbor.get((int) (v / numParts))) {
                    ok.set(u);
                    ov.set(v);
                    mos.write(ok, ov, "sep/part");
                    numCcEdges++;
                }
                else allPairs.add(new LongPair(v, u));
            }
            allPairs.sort(null);

            long cc = -1;
            int cc_part;
            long[] heads = new long[numParts];
            for (LongPair pair : allPairs) {
                long u = pair.u;
                long v = pair.v;

                if (u != cc) {
                    cc = u;
                    cc_part = (int) (cc % numParts);
                    Arrays.fill(heads, -1);
                    heads[cc_part] = cc;
                }
                
                int v_part = (int) (v % numParts);

                ok.set(v);
                ov.set(cc);

                if (heads[v_part] == -1) {
                    heads[v_part] = v;
                    if (!uf.tag(v)) {
                        // Tag the edge, if not changed.
                        // v_part != cc_part if head[v_part] == -1 because head[cc_part] is initialized to cc.
                        if (v_part == pid) ov.set(~cc);
                        else ok.set(~v);
                    }

                    // The edge is changed if both ov and ok have no tags.
                    if(ov.get() >= 0 && ok.get() >= 0) numChanges++;
                    mos.write(ok, ov, "out/part");
                    numEdges++;
                } else {
                    ov.set(heads[v_part]);
                    if (v_part == pid) {
                        if(!hasChild.get((int) (v / numParts))) {
                            // case 1: emit this edge to `sep` (as `in`)
                            mos.write(ok, ov, "sep/part");
                            numInEdges++;
                        }
                        else {
                            mos.write(ok, ov, "out/part");
                            numEdges++;
                        }
                    }
                    else {
                        mos.write(ok, ov, "out/part");
                        numChanges++;
                        numEdges++;
                    }
                }
            }
            
            context.getCounter(Counters.NUM_CHANGES).increment(numChanges);
            context.getCounter(Counters.CC_NUM_EDGES).increment(numCcEdges);
            context.getCounter(Counters.REMOVED_EDGES).increment(numRemovedEdges);
            context.getCounter(Counters.IN_NUM_EDGES).increment(numInEdges);
            context.getCounter(Counters.NUM_EDGES).increment(numEdges);
        }
        
        private long pure(long u) {
            return u < 0 ? ~u : u;
        }
    }
}