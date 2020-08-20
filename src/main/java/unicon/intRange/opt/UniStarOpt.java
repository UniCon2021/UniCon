package unicon.intRange.opt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import utils.IntPairWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

public class UniStarOpt extends Configured implements Tool {

    private Path input;
    private Path output;
    private final String title;
    public long numChanges, numEdges, numInEdges, numCcEdges, numRemovedEdges, intactEdges;
    static private int round;

    public UniStarOpt(Path input, Path output, int round) {
        this.input = input;
        this.output = output;
        this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
        this.round = round;
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), title);
        job.setJarByClass(UniStarOpt.class);
        job.setMapperClass(UniStarMapper.class);
        job.setReducerClass(UniStarReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntPairWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
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
        this.intactEdges = job.getCounters().findCounter(Counters.INTACT_EDGES).getValue();
        return 0;
    }

    public static class UniStarMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntPairWritable> {

        IntWritable ok = new IntWritable();
        IntPairWritable ov = new IntPairWritable();
        int numParts;
        long intact_edge_filtering;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", 0);
            intact_edge_filtering = 0;
        }

        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            int u = key.get();
            int v = value.get();
            ov.set(u, v);
            int up = pure(u) % numParts;
            int vp = pure(v) % numParts;
            int count = 0;

            if (u >= 0) {
                ok.set(up);
                context.write(ok, ov);
                count++;
                
                if(up != vp && v >= 0) {
            		ok.set(vp);
            		context.write(ok, ov);
                    count++;
            	}
            } else {
            	ok.set(vp);
            	context.write(ok, ov);
                count++;
            }
            if (up != vp && count != 2) {
                intact_edge_filtering++;
            }
        }

        private int pure(int u) {
            return u < 0 ? ~u : u;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.INTACT_EDGES).increment(intact_edge_filtering);
        }
    }

    public static class UniStarReducer extends Reducer<IntWritable, IntPairWritable, IntWritable, IntWritable> {

        IntWritable ok = new IntWritable();
        IntWritable ov = new IntWritable();
        int numParts;
        int numNodes;
        private MultipleOutputs<IntWritable, IntWritable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numParts = context.getConfiguration().getInt("numPartitions", -1);
            numNodes = context.getConfiguration().getInt("numNodes", -1);
            mos = new MultipleOutputs<>(context);
        }

        @Override
		protected void cleanup(Reducer<IntWritable, IntPairWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}

		@Override
        protected void reduce(IntWritable key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
            int pid = key.get();

            RemCUF uf = new RemCUF(numNodes, pid, numParts);
            long numChanges = 0;
            long numEdges = 0;
            long numInEdges = 0;
            long numCcEdges = 0;
            long numRemovedEdges = 0;
            
            BitSet hasChild = new BitSet();
            BitSet hasPureNeighbor = new BitSet(); 
            
            // union
            // assume that pure(val.u) > pure(val.v)
            for (IntPairWritable val : values) {
            	if(pure(val.v) % numParts == pid) {
            		int v_local = pure(val.v) / numParts;
            		if(val.u >= 0) hasPureNeighbor.set(v_local);
            		hasChild.set(v_local);
            	}
            	if(pure(val.u) % numParts == pid) {
            		int u_local = pure(val.u) / numParts;
            		if(val.v >= 0) hasPureNeighbor.set(u_local);
            	}
                uf.union(pure(val.u), pure(val.v));
            }

            // refine & emit: need to be optimized
            ArrayList<Long> allPairs = new ArrayList<>();
            for (IntPairWritable pair : uf.p) {
                int u = pair.u;
                int v = pure(uf.find(u));
                
                if(u == v) continue;
                
                int u_local = u / numParts;
                
            	if(u % numParts == pid && !hasPureNeighbor.get(u_local) && !hasChild.get(u_local)) {
                	// case 3: do not emit this edge
                	numRemovedEdges++;
                }
                else if(v % numParts == pid && !hasPureNeighbor.get(v / numParts)) {
                	// case 2: emit this edge to `sep` (as `cc`)
                	ok.set(u);
                	ov.set(v);
                	mos.write(ok,  ov,  "sep/part");
                	numCcEdges++;
                }
                else allPairs.add(((long) v) << 32 | u);
            }
            allPairs.sort(null);

            int cc = -1;
            int[] heads = new int[numParts];
            for (long pair : allPairs) {
                int u = (int) (pair >> 32); // no tag
                int v = (int) pair; // no tag

                if (u != cc) {
                    cc = u;
                    Arrays.fill(heads, -1);
                    heads[cc % numParts] = cc;
                }
                
                int v_part = v % numParts;

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
                		if(!hasChild.get(v / numParts)) {
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
        
        private int pure(int u) {
            return u < 0 ? ~u : u;
        }
    }
}