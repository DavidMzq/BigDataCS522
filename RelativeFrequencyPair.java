import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelativeFrequencyPair {

	public static class RelativeFrequencyPairMapper extends	Mapper<LongWritable, Text, Pair, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// initiate
			HashMap<Pair, Integer> H = new HashMap<Pair, Integer>();
			String[] allTerms = value.toString().split(" ");
//			for(String tmpStr:allTerms)
//				System.out.println(tmpStr);

			// map
			for (int i = 0; i < allTerms.length; i++) 
			{
				String w = allTerms[i];
				Pair pStar=new Pair(w, "*");
				if(!H.containsKey(pStar))
				{
					H.put(pStar,1);
				}
				else
				{
					int newValue=H.get(pStar)+1;
					H.put(pStar, newValue);
				}
				for (int j = i+1; j < allTerms.length; j++) 
				{
					if(allTerms[j].equals(w))
						break;
					
					String u = allTerms[j];
					Pair p = new Pair(w, u);
					if (!H.containsKey(p))//not contain
						H.put(p, 1);
					else
					{
						int newValue = H.get(p) + 1;
						H.put(p, newValue);
					}	
				}
			}
			
			// close
			for (Pair p : H.keySet()) {
				IntWritable x = new IntWritable(H.get(p));
				System.out.println(p.toString()+";"+x.toString());
				context.write(p, x);
			}
		}
	}

	public static class RelativeFrequencyPairReducer extends
			Reducer<Pair, IntWritable, Pair, DoubleWritable> {
		
		Integer marginal = 0;
		
		@Override
		public void reduce(Pair pair, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// initiate
			

			// reduce
//			System.out.println(pair.toString());
//			for(IntWritable tmpInt:values)
//				System.out.println(tmpInt.get());
			//System.out.println(pair.toString());
			
			

			if (pair.getValue().toString().equals("*")) {
				for(IntWritable x : values)
					marginal+=x.get();
				System.out.println(marginal);
			} else {
				double sum = 0;
				double relativeFrequency = 0.0;
				for (IntWritable x : values) {
					sum += x.get();
				}
				System.out.println(marginal);
				relativeFrequency = sum / marginal;
				//System.out.println(relativeFrequency);
				DoubleWritable relativeFrequencyDoubleWritable = new DoubleWritable(relativeFrequency);
				context.write(pair, relativeFrequencyDoubleWritable);
			}
		}
	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(RelativeFrequencyPair.class);

		FileInputFormat.addInputPath(job, new Path(
				"/home/cloudera/workspace/RelativeFrequencies_Pairs/src.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/cloudera/workspace/RelativeFrequencies_Pairs/output"));

		job.setMapperClass(RelativeFrequencyPairMapper.class);
//		job.setCombinerClass(RelativeFrequencyPairReducer.class);
		job.setReducerClass(RelativeFrequencyPairReducer.class);

//		job.setOutputKeyClass(Pair.class);
//		job.setOutputValueClass(IntWritable.class);
		
	    job.setMapOutputKeyClass((Pair.class));
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}