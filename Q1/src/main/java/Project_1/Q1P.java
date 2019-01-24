package Project_1;

/**
 * 
 * Implementation for the first problem of Project One using 
 * Hadoop's MapReduce API. Problem 1 identifies the countries 
 * where the percent of female graduates is less than 30%
 * 
 */

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
	/*
	 * 
	 * The main method executes the MapReduce job to be implemented
	 * in Hadoop. It sets the MapperClass and ReduceClass as well as the
	 * OutputKeyClass as Text and OutputValueClass as DoubleWritable. 
	 * 
	 */
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
		/*
		 * Map Class parses through data to search for the SE.PRM.CMPL.FE.ZS
		 * and SE.TER.CMPL.FE.ZS codes to identify rows which contain data
		 * pertinent to primary female graduates and tertiary graduates.
		 * If the value is less than 30% in the last year of data available,
		 * it is mapped to a key containing the code, country, and year.
		 * 
		 * This mapper only looks for data in the last five years for a country.
		 * If data is incomplete, it is not mapped.
		 * 
		 * @param LongWritable key
		 * @param Text value
		 * @param Context con
		 * 
		 * @return <Text key, DoubleWritable value>
		 */
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String dataCSV = value.toString();
			String[] lines=dataCSV.split("\n");
		
			for(String line: lines ){
				String[] columns = line.split("\",\"");
				String code = columns[3];
				if(code.matches("SE.PRM.CMPL.FE.ZS|SE.TER.CMPL.FE.ZS")){
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
		/*
		 * 
		 * Reduce class just to sort the key, value pairs from the mapper 
		 * alphabetically
		 * @param <Text key, DoubleWritable value>
		 * @return <Text key, DOubleWritable value>
		 * 
		 */
		public void reduce(Text word, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
			for(DoubleWritable value : values){
				con.write(word, value);
			}
		}
	}
}