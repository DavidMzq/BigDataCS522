import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelativeFrequencyStripes {
	private final static IntWritable one = new IntWritable(1);
	
	public static class RelativeFrequencyStripeMapper extends	Mapper<LongWritable, Text, Text, MapWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] allTerms = value.toString().split(" ");

			// map
			for (int i = 0; i < allTerms.length; i++) 
			{
				HashMap<String,Integer> h = new HashMap<String,Integer>();
				String w = allTerms[i];
				
				for (int j = i+1; j < allTerms.length; j++) 
				{
					String u=allTerms[j];
					if (w.equals(u)) 	
						break;
					
					String hKeyStr="";
					for(String x:h.keySet())
					{
						hKeyStr+=x.toString()+",";	
					}
					//System.out.println("elements in h:"+hKeyStr);
					
					if(!h.containsKey(u))//contains no 
					{
						h.put(u,1);
					}
					else
					{
						int newValue=h.get(u)+1;
						h.put(u,newValue);
					}
				 }
				String stripeString="";
				MapWritable hWritable=new MapWritable();
				hWritable=ConvertMap(h);
				stripeString=GetStripeStringAnalyzeWritable(hWritable);
				System.out.println("MaperOutput:");
				System.out.println(w+","+stripeString);
				context.write(new Text(w), hWritable);	
			}	
			
		}
	}

	
	public static class RelativeFrequencyStripeReducer extends
			Reducer<Text, MapWritable, Text, Text> {
		// initiate
		Integer marginal = 0;
		private final static IntWritable zero = new IntWritable(0);
		
		
		@Override
		public void reduce(Text w, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {	
			MapWritable hf = new MapWritable();
			
			int marginal=0;
			
			for(MapWritable h:values)
				for(Writable u:h.keySet())
				{
					if(!hf.containsKey(u))
						hf.put(u,zero);
					int sumValue=((IntWritable)(hf.get(u))).get()+ ((IntWritable)(h.get((Text)u))).get();
					IntWritable x = new IntWritable(sumValue);
					hf.put(u, x);
					marginal = marginal + ((IntWritable)(h.get((Text)u))).get();
				}
			
			
			for(Writable u:hf.keySet())
			{
				Double frequency = ((IntWritable)hf.get(u)).get()/(double)marginal;
				//System.out.println(frequency);
				DoubleWritable f = new DoubleWritable(frequency);
				hf.put(u, f);			
			}
			//System.out.println("hf Size:"+hf.size());
			
			
			String stripeString="";
			stripeString=GetStripeStringAnalyzeWritable(hf);
			System.out.println("Reducer Output:");
			System.out.println(w+","+stripeString);
			context.write(w, new Text(stripeString));
		}
	}

	
	public static MapWritable ConvertMap(HashMap<String,Integer> hashMap)
	{
		MapWritable resultMap=new MapWritable();
		for(String tmpKey:hashMap.keySet())
		{
			IntWritable n = new IntWritable(hashMap.get(tmpKey));
			resultMap.put(new Text(tmpKey), n);
		}
			return resultMap;
	}
	
	public static  String GetStripeStringAnalyzeWritable(MapWritable h)
	{
		String stripeString="";
		for(Writable key:h.keySet())
		{
			String elmStr="";
			elmStr="("+key.toString()+","+ h.get(key).toString()+")";
			stripeString+=elmStr+",";
		}
		//remove last comma,add brace
		if(stripeString.length()>0)
			stripeString=stripeString.substring(0,stripeString.length()-1);
		stripeString="{"+stripeString+"}";
		return stripeString;
		
	}

	public static void main(String[] args) throws Exception {
		Runtime.getRuntime().exec("rm -rf /home/cloudera/workspace/RelativeFrequency/output");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(RelativeFrequencyStripes.class);

		FileInputFormat.addInputPath(job, new Path(
				"/home/cloudera/workspace/RelativeFrequency/src.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/cloudera/workspace/RelativeFrequency/output"));

		job.setMapperClass(RelativeFrequencyStripeMapper.class);
		job.setReducerClass(RelativeFrequencyStripeReducer.class);

	    job.setMapOutputKeyClass((Text.class));
        job.setMapOutputValueClass(MapWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

       
        
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