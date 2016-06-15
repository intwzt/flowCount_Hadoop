package snnu.hadoop.flowsort;

import java.io.IOException;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class flowCount {
	
	
	public static class flowCountMapper extends Mapper<LongWritable, Text, Text, flowBean>{
		
		private Text k =  new Text();
		private flowBean bean = new flowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fieds = StringUtils.split(line, "\t");
			
			String id = fieds[0];
			Long up_flow = Long.parseLong(fieds[fieds.length - 3]);
			Long down_flow = Long.parseLong(fieds[fieds.length - 2]);
			bean.set(id, up_flow, down_flow);
			k.set(id);
			
			context.write(k, bean);
			
		}
	}
	
	public static class flowCountReducer extends Reducer<Text, flowBean, Text, flowBean>{
		
		private flowBean bean = new flowBean();
		
		@Override
		protected void reduce(Text key, Iterable<flowBean> values, Context context)
				throws IOException, InterruptedException {
			
			long up = 0;
			long down = 0;
			for( flowBean value : values){
				
				up += value.getUp_flow();
				down += value.getDown_flow();
				
			}
			
			bean.set(key.toString(), up, down);
			
			context.write(key, bean);
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(flowCount.class);
		
		job.setMapperClass(flowCountMapper.class);
		job.setReducerClass(flowCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(flowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
	

}
