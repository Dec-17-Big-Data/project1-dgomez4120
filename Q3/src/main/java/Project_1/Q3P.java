package Project_1;

/*
 * 
 * Implementation for Problem three of Project one.
 * Problem three asks to list the percent change in male employment
 * since the year 2000 for the United States. To do this, only a
 * mapper was used to map the code and year as the key and the value 
 * being the percent of male employment for that year.
 * 
 * Similar to Problem two, a try-catch was used to skip over records 
 * that contained no data. two codes were partitioned from the Gender_StatsData
 * to explore the issue of male employment: SL.TLF.CACT.MA.NE.ZS and 
 * SL.TLF.CACT.MA.ZS.
 * 
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3P {
	public static void main(String [] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"Q3");
		j.setJarByClass(Q3P.class);
		j.setMapperClass(MapForQ3.class);
		j.setNumReduceTasks(0);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	public static class MapForQ3 extends Mapper<LongWritable, Text, Text, FloatWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String csv = value.toString();			
			String[] lines=csv.split("\",\n");
			
			for(String line: lines ){
				String[] columns=line.split("\",\"");
				int columnsLength = columns.length;
				String code = columns[3];
				String cCode = columns[1];
				if(code.matches("SL.TLF.CACT.MA.NE.ZS|SL.TLF.CACT.MA.ZS") && cCode.equals("USA")){
					for(int i = 1; i<=17; i++){
						try{
							String stringValue = columns[columnsLength-i].replace("\"", "").replace(",", "");
							float floatValue = Float.valueOf(stringValue);
							String year = Integer.toString(2017-i);
							String country = columns[0].replace("\"", "");
							String keyString = country+" "+code+" "+year;
							Text outputKey = new Text(keyString);
							FloatWritable outputValue = new FloatWritable(floatValue);
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
