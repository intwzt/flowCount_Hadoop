package snnu.hadoop.flowsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class flowCountSort {
	
	public static class flowCountSortMapper extends Mapper<LongWritable, Text,  flowBean, NullWritable>{
		
		private flowBean bean = new flowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fieds = StringUtils.split(line, "\t");
			
			String id = fieds[0];
			Long up_flow = Long.parseLong(fieds[1]);
			Long down_flow = Long.parseLong(fieds[2]);
			
			bean.set(id, up_flow, down_flow);
			
			context.write(bean, NullWritable.get());
			
		}
		
	}
	
	public static class flowCountSortReducer extends Reducer<flowBean, NullWritable, Text, flowBean>{
		
		@Override
		protected void reduce(flowBean key, Iterable<NullWritable> value, Context context)
				throws IOException, InterruptedException {
			
			context.write(new Text(key.getId()), key);
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(flowCountSort.class);
		
		job.setMapperClass(flowCountSortMapper.class);
		job.setReducerClass(flowCountSortReducer.class);
		
		job.setMapOutputKeyClass(flowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(flowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
	
}
