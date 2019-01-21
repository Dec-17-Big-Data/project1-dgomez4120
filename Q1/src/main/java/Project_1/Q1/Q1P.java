package Project_1.Q1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1P {
	public static void main(String [] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"Q1");
		j.setJarByClass(Q1P.class);
		j.setMapperClass(MapForQ1.class);
		j.setReducerClass(ReduceForQ1.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	public static class MapForQ1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String dataCSV = value.toString();
			String[] lines=dataCSV.split("\n");
		
			for(String line: lines ){
				String[] columns = line.split("\",\"");
				String code = columns[3];
				if(code.matches("SE.PRM.CMPL.FE.ZS|SE.SEC.CMPL.FE.ZS|SE.TER.CMPL.FE.ZS")){
					int columnsLength = columns.length;
					for(int i = 1; i<=5; i++){
						try{
							String stringValue = columns[columnsLength-i].replace("\"", "");
							double doubleValue = Double.valueOf(stringValue);
							if(doubleValue < 30.0){
								String year = Integer.toString(2017-i);
								String country = columns[0].replace("\"", "");
								Text outputKey = new Text(code+" "+country+" "+year);
								DoubleWritable outputValue = new DoubleWritable(doubleValue);
								con.write(outputKey, outputValue);
							}
							break;
						}catch(NullPointerException e){
							continue;
						}catch(NumberFormatException e){
							continue;
						}
					}
				}
			}
		}
	}
	public static class ReduceForQ1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		public void reduce(Text word, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
			for(DoubleWritable value : values){
				con.write(word, value);
			}
		}
	}
}