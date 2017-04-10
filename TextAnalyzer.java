package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import javax.xml.soap.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 	
public class TextAnalyzer {
 	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Writable> {
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, HashWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			HashMap<String, HashMap<String, Integer>> lineOutput = new HashMap<String, HashMap<String, Integer>>();
			HashMap<String, Integer> masterCount = new HashMap<String, Integer>();
			
			while (tokenizer.hasMoreTokens()) {
				String rawWord = tokenizer.nextToken();
				rawWord.toLowerCase();
				rawWord.replaceAll("[^a-zA-Z0-9]", " ");
				if(!masterCount.containsKey(rawWord)) {
					masterCount.put(rawWord, 1);
				} else {
					int current = masterCount.get(rawWord);
					masterCount.put(rawWord, current+1);
				}
			}
			
			//Loop this for the Map
			for(String rawWord : masterCount.keySet()) {
				HashMap<String, Integer> revisedCount = new HashMap<String, Integer>();
				revisedCount.putAll(masterCount);
				if(revisedCount.get(rawWord) == 1) {
					revisedCount.remove(rawWord);
				} else {
					int current = revisedCount.get(rawWord);
					revisedCount.put(rawWord, current-1);
				}
				word.set(rawWord);
				output.collect(word, new HashWritable(revisedCount));
			}
	}
	
	public static class HashWritable implements Writable {
		public HashMap<String, Integer> hash;
		
		public HashWritable(HashMap<String, Integer> hash) {
			this.hash = hash;
		}
		
		@Override
		public void readFields(DataInput arg0) throws IOException {
			//How to use this correctly?
			for(String word : hash.keySet()) {
				//String writable?
				IntWritable intWrite = new IntWritable(hash.get(word));
				intWrite.readFields(arg0);
			}
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// Is this even necessary?
			for(String word : hash.keySet()) {
				//word.write(arg0);
				IntWritable intWrite = new IntWritable(hash.get(word));
				intWrite.write(arg0);
			}
		}
		
		public HashMap<String, Integer> get() {
			return hash;
		}
	}


	public static class Reduce extends MapReduceBase implements Reducer<Text, Writable, Text, Writable> {
		public void reduce(Text key, Iterator<HashWritable> values, OutputCollector<Text, HashWritable> output, Reporter reporter) throws IOException {
			HashMap<String, Integer> masterHash;
			HashMap<String, Integer> compareHash;
			if(values.hasNext()) {
				masterHash = values.next().get();
			}
			while (values.hasNext()) {
				compareHash = values.next().get();
				for(String word : masterHash.keySet()) {
					if(compareHash.containsKey(word)) {
						int val = masterHash.get(word) + compareHash.get(word);
						masterHash.put(word, val);
						compareHash.remove(word);
					}
				}
				masterHash.putAll(compareHash);
			}
			output.collect(key, new HashWritable(masterHash));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TextAnalyzer.class);
		conf.setJobName("textanalyzer");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(HashWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
	
	}
}
