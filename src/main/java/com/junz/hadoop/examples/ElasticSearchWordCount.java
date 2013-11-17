package com.junz.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.mr.ESOutputFormat;


public class ElasticSearchWordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, NullWritable, MapWritable> {
		private IntWritable result = new IntWritable();
		private MapWritable map = new MapWritable();
		private static Text WORD = new Text("word");
		private static Text COUNT = new Text("count");
		private static Text ID = new Text("id");
		private static Text DATE = new Text("2013-11-20");
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			map.put(ID, DATE);
			map.put(WORD, key);
			map.put(COUNT, result);
			context.write(NullWritable.get(), map);
			map.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: wordcount <in>");
			System.exit(2);
		}
		conf.set("es.resource", "yaohui/wc");
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		Job job = new Job(conf, "word count");
		job.setJarByClass(ElasticSearchWordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setOutputFormatClass(ESOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(MapWritable.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
