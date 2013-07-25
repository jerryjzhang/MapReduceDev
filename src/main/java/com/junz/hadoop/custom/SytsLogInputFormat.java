/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.junz.hadoop.custom;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * A InputFormat that reads input data from an SQL table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as key and
 * DBWritables as value.
 * 
 * The SQL query, and input class can be using one of the two setInput methods.
 */
public class SytsLogInputFormat extends InputFormat<LongWritable, Text> {
	public static final String START_ID_PROPERTY = "mapred.syts.start.id";
	public static final String NUMBER_LOG_PROPERTY = "mapred.syts.number.id";
	public static final String NUMBER_MAP_PROPERTY = "mapred.syts.number.map";
	
	/**
	 * A RecordReader that reads records from a SQL table. Emits LongWritables
	 * containing the record number as key and DBWritables as value.
	 */
	protected class SytsLogRecordReader extends
			RecordReader<LongWritable, Text> {
		private Configuration job;

		private DBInputSplit split;

		private long pos = 0;

		/**
		 * @param split
		 *            The InputSplit to read data for
		 * @throws SQLException
		 */
		protected SytsLogRecordReader(InputSplit split, TaskAttemptContext context)
				throws SQLException {
			this.split = (DBInputSplit)split;
			this.job = context.getConfiguration();
			pos = this.split.getStart();
		}

		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// do nothing
		}
		
		  /**
		   * Read the next key, value pair.
		   * @return true if a key/value pair was read
		   * @throws IOException
		   * @throws InterruptedException
		   */
		  public  
		  boolean nextKeyValue() throws IOException, InterruptedException{
			  if(pos == split.getEnd()){ return false; }
			  pos++;
			  return true;
		  }

		  /**
		   * Get the current key
		   * @return the current key or null if there is no current key
		   * @throws IOException
		   * @throws InterruptedException
		   */
		  public 
		  LongWritable getCurrentKey() throws IOException, InterruptedException{
			  return new LongWritable(1);
		  }

		  /**
		   * Get the current value.
		   * @return the object that was read
		   * @throws IOException
		   * @throws InterruptedException
		   */
		  public  
		  Text getCurrentValue() throws IOException, InterruptedException{
			 return new Text("jun"); 
		  }
		/**
		 * Close this {@link InputSplit} to future operations.
		 * 
		 * @throws IOException
		 */
		public void close() throws IOException {

		}

		/** {@inheritDoc} */
		public float getProgress() throws IOException {
			return pos / (float) split.getLength();
		}
	}

	public  
    RecordReader<LongWritable,Text> createRecordReader(InputSplit split,
                                         TaskAttemptContext context
                                        ) throws IOException, 
                                                 InterruptedException{
		try {
			return new SytsLogRecordReader((DBInputSplit) split, context);
		} catch (SQLException ex) {
			throw new IOException(ex.getMessage());
		}
	}

	  public List<InputSplit> getSplits(JobContext context
	                               ) throws IOException, InterruptedException {
		  Configuration job = context.getConfiguration();
		  List<InputSplit> splits = new ArrayList<InputSplit>();
		  
		try {
			long startId = job.getLong( START_ID_PROPERTY,
					1);
			long numberOfIds = job.getLong(
					 NUMBER_LOG_PROPERTY, 1);			
			int groups = job.getInt( NUMBER_MAP_PROPERTY, 1);
			long groupSize = (numberOfIds / groups);

			// Split the rows into n-number of chunks and adjust the last chunk
			// accordingly
			for (int i = 0; i < groups; i++) {
				DBInputSplit split;

				if ((i + 1) == groups)
					split = new DBInputSplit(i * groupSize + startId,
							numberOfIds + startId);
				else
					split = new DBInputSplit(i * groupSize + startId,
							(i * groupSize) + groupSize + startId);

				splits.add(split);
			}

			return splits;
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}

	public static void setStartId(Job job, long id) {
		job.getConfiguration().setLong( START_ID_PROPERTY,
				id);
	}

	public static void setNumberOfIds(Job job, long number) {
		job.getConfiguration().setLong(
				 NUMBER_LOG_PROPERTY, number);
	}
	
	public static void setNumberOfMaps(Job job, long number) {
		job.getConfiguration().setLong(
				 NUMBER_MAP_PROPERTY, number);
	}
}
