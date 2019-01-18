package Project_1.Q4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q4Count {
	public static void main(String [] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"wordcount");
		j.setJarByClass(Q4Count.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, FloatWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String csv = value.toString();			
			String[] lines=csv.split("\",\n");
			
			for(String line: lines ){
				String[] columns=line.split("\",\"");
				int columnsLength = columns.length;
				String code = columns[3].replace("\"", "");
				if(code.matches("SL.TLF.CACT.FE.NE.ZS|SL.TLF.CACT.FE.ZS") && columns[1].equals("USA")){
					for(int i = 1; i<=17; i++){
						try{
							String stringValue = columns[columnsLength-i].replace("\"", "");
							float floatValue = Float.valueOf(stringValue);
							String year = Integer.toString(2017-i);
							String keyString = columns[0]+" "+columns[3]+" "+year;
							Text outputKey = new Text(keyString);
							FloatWritable outputValue = new FloatWritable(floatValue);
							con.write(outputKey, outputValue);
						}catch(NumberFormatException e){
							continue;
						}
					}
				}
			}
		}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, FloatWritable>{
		public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException{
			int sum = 0;
			for(FloatWritable value : values){
				sum += value.get();
			}
			con.write(word, new FloatWritable(sum));
		}
	}
}
