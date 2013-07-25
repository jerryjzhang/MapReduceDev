package com.junz.hadoop.examples;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.junz.hadoop.examples.JunzWordCount;


public class JunzWordCountTest {
	private static final IntWritable ONE = new IntWritable(1);

	@Test
	public void testMap() {
		Text value = new Text("zhang jun");
		new MapDriver<Object, Text, Text, IntWritable>()
			.withMapper(new JunzWordCount.TokenizerMapper())
			.withInputValue(value)
			.withOutput(new Text("zhang"), ONE)
			.withOutput(new Text("jun"), ONE)
			.runTest();
	}
	
	@Test
	public void testReduce() {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(ONE);
		values.add(ONE);
		new ReduceDriver<Text,IntWritable,Text,IntWritable>()
			.withReducer(new JunzWordCount.IntSumReducer())
			.withInput(new Text("zhang"), values)
			.withOutput(new Text("zhang"), new IntWritable(2))
			.runTest();
	}

}
