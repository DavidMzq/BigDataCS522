import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

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

	public static class RelativeFrequencyPairMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
			// initiate
			HashMap<Pair, Integer> H = new HashMap<Pair, Integer>();
			String[] allTerms = value.toString().split("\\s+");
			
			System.out.println("===Output of one record/line==="); //one line as the input unit
			for (int i = 0; i < allTerms.length; i++) 
			{
				System.out.print(allTerms[i]+" ");
			}
			System.out.println("\n===Output of one record/line===END");
			
			// map
			for (int i = 0; i < allTerms.length; i++) 
			{
				String w = allTerms[i];
				Pair pStar=new Pair(w, "*");
				if(!H.containsKey(pStar))
				{
					H.put(pStar,0);
				}
				/*
				else
				{
					int newValue=H.get(pStar)+1;
					H.put(pStar, newValue);
				}
				*/
				for (int j = i+1; j < allTerms.length; j++) 
				{
					if(allTerms[j].equals(w))
						break;
					
					String u = allTerms[j];
					Pair p = new Pair(w, u);
					int newValue=H.get(pStar)+1;
					H.put(pStar, newValue);
					
					if (!H.containsKey(p))  //not contain
						H.put(p, 1);
					else
					{
						newValue = H.get(p) + 1;
						H.put(p, newValue);
					}	
				}
			}
			
			// close
		
			System.out.println("===Mapper Result in Hashmap===");
			for (Pair p : H.keySet()) {
				IntWritable count = new IntWritable(H.get(p));
				System.out.println(p.toString()+", Count "+count.toString());
				context.write(p, count);
			}
			System.out.println("===Mapper Result in Hashmap===END");
		}
	}

	public static class RelativeFrequencyPairReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
		Integer marginal = 0;
		//Text tPreviousLeft=new Text("");
		String sPreviousLeft=new String("");
		@Override
		public void reduce(Pair pair, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException {
			// initiate
			/*
			System.out.println("===Reducer Input===");
			System.out.println("Pair:");
			System.out.println(pair.toString());
			
			System.out.print("Pair values List:");
			for(IntWritable x : values)
				System.out.print(x+" ");
		
			System.out.println("\n===Reducer Input===END");
			*/
			//reduce
			//if(!pair.getLeft().equals(tPreviousLeft)){
			if(!pair.getLeft().toString().equals(sPreviousLeft)){
				//tPreviousLeft=pair.getLeft();
				sPreviousLeft=pair.getLeft().toString();
				marginal=0;
			}
			if (pair.getRight().toString().equals("*")) {
				for(IntWritable x : values)
					marginal+=x.get();
				System.out.println("Pair("+pair.getLeft().toString()+","+pair.getRight().toString()+") "+"marginal: "+marginal);
				
			} 
			else {
				double sum = 0;
				double relativeFrequency = 0.0;
				for (IntWritable x : values) {
					sum += x.get();
				}
				relativeFrequency = sum/marginal;
				DoubleWritable relativeFrequencyDoubleWritable = new DoubleWritable(relativeFrequency);
				context.write(pair, relativeFrequencyDoubleWritable);
				
			}
			//
			
		}//reduce
	} //RelativeFrequencyPairReducer

	public static void main(String[] args) throws Exception {
		Runtime.getRuntime().exec("rm -rf /home/cloudera/workspace/RelativeFrequency/output");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(RelativeFrequencyPair.class);

		FileInputFormat.addInputPath(job, new Path(	"/home/cloudera/workspace/RelativeFrequency/src.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/workspace/RelativeFrequency/output"));

		job.setMapperClass(RelativeFrequencyPairMapper.class);
//		job.setCombinerClass(RelativeFrequencyPairReducer.class);
		job.setReducerClass(RelativeFrequencyPairReducer.class);

	    job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
            
		
		//Output the content of reduce output to console as well, thus don't need to open it to check
        if(job.waitForCompletion(true)){
        File fileout=new File("/home/cloudera/workspace/RelativeFrequency/output/part-r-00000");
        
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileout)));
    	String data = null;
    	String sFileLines ="";
    	while((data = br.readLine())!=null)
    	{
        sFileLines=data; //
        //sFileLines+="\r\n"; //
        System.out.println(sFileLines);
       }
       br.close();
	  }
      //Output the content of red END
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


