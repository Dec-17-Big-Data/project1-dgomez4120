package Project_1;

/*
 * Implementation for Problem two of Project one where
 * the average increase in female education for the United
 * States is listed from the year 2000.
 * 
 * Only a mapper was used for this problem that'd map the 
 * year and code to the percent of females enrolled in either primary,
 * secondary, or tertiary education.
 * 
 * If there was no value input in the record for a specific year,
 * a try-catch was implemented that'd catch the NumberFormatException
 * and continue the for-loop to the next year or row.
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2P {
	public static void main(String [] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"Q2");
		j.setJarByClass(Q2P.class);
		j.setMapperClass(MapForQ2.class);
		j.setNumReduceTasks(0);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	public static class MapForQ2 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String csv = value.toString();			
			String[] lines=csv.split("\",\n");
			
			for(String line: lines ){
				String[] columns=line.split("\",\"");
				int columnsLength = columns.length;
				String code = columns[3];
				String cCode = columns[1];
				if(code.matches("SE.PRM.ENRR.FE|SE.SEC.ENRR.FE|SE.TER.ENRR.FE") && cCode.equals("USA")){
					for(int i = 1; i<=17; i++){
						try{
							String stringValue = columns[columnsLength-i].replace("\"", "").replace(",", "");
							Double floatValue = Double.valueOf(stringValue);
							String year = Integer.toString(2017-i);
							String country = columns[0].replace("\"", "");
							String keyString = country+" "+code+" "+year;
							Text outputKey = new Text(keyString);
							DoubleWritable outputValue = new DoubleWritable(floatValue);
							con.write(outputKey, outputValue);
							continue;
						}catch(NumberFormatException e){
							continue;
						}
					}
				}
			}
		}
	}
}
