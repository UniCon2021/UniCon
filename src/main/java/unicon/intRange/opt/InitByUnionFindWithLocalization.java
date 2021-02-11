package unicon.intRange.opt;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import utils.ExternalSorter;
import utils.TabularHash;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class InitByUnionFindWithLocalization extends Configured implements Tool{

	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
	public long outputSize;

	public InitByUnionFindWithLocalization(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
	}

	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, title);
		job.setJarByClass(this.getClass());
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(InitializationMapper.class);
		job.setReducerClass(LocalizationReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setPartitionerClass(LocalizationPartitioner.class);

		job.waitForCompletion(verbose);

		outputSize = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();

		return 0;
	}

	public static long code(long n, int p){
		return (n << 10) | p;
	}

	public static long decode_id(long n_raw){
		return n_raw >>> 10;
	}

	public static int decode_part(long n_raw){
		return (int) (n_raw & 0x3FF);
	}

	static public class InitializationMapper extends Mapper<Object, Text, LongWritable, LongWritable>{
		int numPartitions;
		Int2IntOpenHashMap parent;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			parent = new Int2IntOpenHashMap();
			parent.defaultReturnValue(-1);
			numPartitions = context.getConfiguration().getInt("numPartitions", 0);
		}

		private void union(int a, int b) {
			int r1 = find(a);
			int r2 = find(b);

			if(r1 > r2) parent.put(r1, r2);
			else if(r1 < r2) parent.put(r2, r1);
		}

		private int find(int x) {
			while (parent.get(x) != -1) {
				int ppx = parent.get(parent.get(x));
				if (ppx != -1) {
					parent.put(x, ppx);
				}
				x = parent.get(x);
			}
			return x;
		}

		LongWritable ou = new LongWritable();
		LongWritable ov = new LongWritable();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer st = new StringTokenizer(value.toString());

			int u = Integer.parseInt(st.nextToken());
			int v = Integer.parseInt(st.nextToken());

			if(find(u) != find(v)) { union(u, v); }
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Int2IntMap.Entry pair : parent.int2IntEntrySet()){
				int u = pair.getIntKey();
				int v = find(pair.getIntValue());

				int upart = u % numPartitions;
				ou.set(code(v, upart));
				ov.set(u);

				context.write(ou, ov);
			}
		}
	}

	static public class LocalizationReducer extends Reducer<LongWritable, LongWritable, IntWritable, IntWritable> {

		ExternalSorter sorter;
		int numParts;

		@Override
		protected void setup(Context context){
			String[] tmpPaths = context.getConfiguration().getTrimmedStrings("yarn.nodemanager.local-dirs");
			sorter = new ExternalSorter(tmpPaths);
			numParts = context.getConfiguration().getInt("numPartitions", -1);
		}

		IntWritable om = new IntWritable();
		IntWritable ov = new IntWritable();

		class PredicateWithMin implements Predicate<Long> {
			long mpu;
			long u;

			PredicateWithMin(long u) {
				this.u = u;
				this.mpu = Long.MAX_VALUE;
			}

			public boolean test(Long v) {
				if(mpu > v) mpu = v;
				return true;
			}
		}

		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long u_raw = key.get();
			long u = decode_id(u_raw);

			PredicateWithMin lfilter = new PredicateWithMin(u);

			Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
					.map(LongWritable::get).filter(lfilter).iterator();

			Iterator<Long> uN_iterator = sorter.sort(it);

			long mpu = lfilter.mpu;

			while(uN_iterator.hasNext()){
				long v = uN_iterator.next();

				if(v != mpu){
					ov.set((int) v);
					om.set((int) mpu);
					context.write(ov, om);
				}
				else{
					ov.set((int) v);
					om.set((int) u);
					context.write(ov, om);
				}
			}
		}
	}

	public static class LocalizationPartitioner extends Partitioner<LongWritable, LongWritable> {
		TabularHash H = TabularHash.getInstance();

		public int hash(long n_raw){
			return H.hash(decode_id(n_raw) + 41 * decode_part(n_raw));
		}

		@Override
		public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
		    return decode_part(key.get());
		}
	}
}