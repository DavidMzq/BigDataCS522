import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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
//				System.out.println(p.toString()+";"+x.toString());
				context.write(p, x);
			}
		}
	}

	
	public static class RelativeFrequencyPairReducer extends
			Reducer<Pair, IntWritable, Pair, DoubleWritable> {
		
		// initiate
		Integer marginal = 0;
		
		@Override
		public void reduce(Pair pair, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {	

			if (pair.getValue().toString().equals("*")) {
				for(IntWritable x : values)
					marginal+=x.get();
				//System.out.println(marginal);
			} else {
				double sum = 0;
				double relativeFrequency = 0.0;
				for (IntWritable x : values) {
					sum += x.get();
				}
//				System.out.println(marginal);
				relativeFrequency = sum / marginal;
				DoubleWritable relativeFrequencyDoubleWritable = new DoubleWritable(relativeFrequency);
				context.write(pair, relativeFrequencyDoubleWritable);
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Runtime.getRuntime().exec("rm -rf /home/cloudera/workspace/RelativeFrequency/output");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(RelativeFrequencyPair.class);

		FileInputFormat.addInputPath(job, new Path(
				"/home/cloudera/workspace/RelativeFrequency/src.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/cloudera/workspace/RelativeFrequency/output"));

		job.setMapperClass(RelativeFrequencyPairMapper.class);
		job.setReducerClass(RelativeFrequencyPairReducer.class);

	    job.setMapOutputKeyClass((Pair.class));
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

       
        
        OutPut(job,"/home/cloudera/workspace/RelativeFrequencyPair/output/part-r-00000");
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	
	//Output the content of reduce output to console as well, thus don't need to open it to check
	public static void OutPut(Job job,String fullFileName) throws ClassNotFoundException, FileNotFoundException, IOException, InterruptedException
	{
        if(job.waitForCompletion(true)){
        File fileout=new File("/home/cloudera/workspace/RelativeFrequencyPair/output/part-r-00000");
	        if(fileout.exists())
	        {
		        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileout)));
		    	String data = null;
		    	String sFileLines ="";
		    	while((data = br.readLine())!=null)
		    	{
		        sFileLines=data; //
		        sFileLines+="\r\n"; //
		        System.out.println(sFileLines);
	       }
	       br.close();
       }
	}
   
	}
	
}