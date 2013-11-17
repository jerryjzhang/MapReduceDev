package com.junz.hadoop.examples;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.mr.ESOutputFormat;

public class ElasticSearchLogExtractor {

	public static class TokenizerMapper extends
			Mapper<Object, Text, NullWritable, MapWritable> {

		private Text timestamp = new Text();
		private Text logmode = new Text();
		private Text className = new Text();
		private Text message = new Text();
		private MapWritable map = new MapWritable();
		private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S");
		private SimpleDateFormat ddf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
		
		private static Text CLASSNAME = new Text("class");
		private static Text LOGMODE = new Text("logmode");
		private static Text TIMESTAP = new Text("@timestamp");
		private static Text MSG = new Text("@message");
		private static Pattern pattern = Pattern
				.compile("(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d,\\d\\d\\d) (\\S+) (\\S+) (.*)");
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Matcher m = pattern.matcher(value.toString());
			if (m.matches()) {
				try{
					timestamp.set(ddf.format(sdf.parse(m.group(1))));
				}catch(ParseException e){
					return;
				}
				map.put(TIMESTAP, timestamp);
				logmode.set(m.group(2));
				map.put(LOGMODE, logmode);
				className.set(m.group(3).replace(":", ""));
				map.put(CLASSNAME, className);
				message.set(m.group(4));
				map.put(MSG, message);
			}
			context.write(NullWritable.get(), map);
			map.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: logextract <in>");
			System.exit(2);
		}
		conf.set("es.resource", "jobtracker/log");
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		Job job = new Job(conf, "word count");
		job.setJarByClass(ElasticSearchLogExtractor.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setOutputFormatClass(ESOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(MapWritable.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main1(String[] args)throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S");
		SimpleDateFormat ddf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
		String date = ddf.format(sdf.parse("2013-08-03 15:02:16,278"));
		
		System.out.println(date);
	}
}
