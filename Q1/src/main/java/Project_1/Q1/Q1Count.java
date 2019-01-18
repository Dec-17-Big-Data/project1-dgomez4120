package Project_1.Q1;

import java.io.IOException;
//import java.io.StringReader;
//import java.util.ArrayList;
//import java.util.List;
//import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q1Count {
	public static void main(String [] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"wordcount");
		j.setJarByClass(Q1Count.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		/*	String csv = value.toString();	
			CSVReader R = new CSVReader(new StringReader(csv));
			List<String[]> lines = new ArrayList<>();
		    lines = R.readAll();
			R.close();
		*/
			String csv = value.toString();			
			String[] rows = csv.split("\n");
			
			for(String row: rows ){
				String[] columnSplit=row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				int columns = columnSplit.length;
				if(columnSplit[3].equals("\"SE.PRM.CMPL.FE.ZS\"")){
					column: for(int i = columns-1; i >= columns-6; i--){
						String field = columnSplit[i].replace("\"", "");
						if(field.equals("")){
							continue column;
						}else if(Float.parseFloat(field) < 30){
							String keyString = columnSplit[0]+" "+columnSplit[3]+" "+columnSplit[i];
							Text outputKey = new Text(keyString);
							IntWritable outputValue = new IntWritable(1);
							con.write(outputKey, outputValue);
							break column;
						}else{
							continue column;
						}
					}
					
				}
				
			}
		}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}
