package com.junz.hadoop.examples;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.junz.hadoop.examples.JunzWordCount;


public class ElasticSearchLogExtractorTest {
	private static final IntWritable ONE = new IntWritable(1);
	private static Text CLASSNAME = new Text("class");
	private static Text LOGMODE = new Text("logmode");
	private static Text TIMESTAP = new Text("@timestamp");
	private static Text MSG = new Text("@message");
	
	@Test
	public void testMap() {
		MapWritable map = new MapWritable();
		map.put(TIMESTAP, new Text("2013-08-03 15:02:16 +0800"));
		map.put(LOGMODE, new Text("INFO"));
		map.put(CLASSNAME, new Text("org.apache.hadoop.mapred.JobTracker:"));
		map.put(MSG, new Text("STARTUP_MSG"));
		
		Text value = new Text("2013-08-03 15:02:16,278 INFO org.apache.hadoop.mapred.JobTracker: STARTUP_MSG");
		new MapDriver<Object, Text, NullWritable, MapWritable>()
			.withMapper(new ElasticSearchLogExtractor.TokenizerMapper())
			.withInputValue(value)
			.withOutput(NullWritable.get(), map)
			.runTest();
	}
}
