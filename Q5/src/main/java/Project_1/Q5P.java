package Project_1;

/*
 * 
 * Implementation for Problem five of Project one.
 * This problem explores the correlation between the years
 * education a woman achieves and the corresponding fertility rate.
 * 
 * The mapper class maps the country/code key to a year:value value
 * where both are of Text class. The year:value Text value was meant 
 * for post-processing in python.
 * 
 * The reducer class concatenates the values for a country/code into 
 * a single Text value delimited by commas. It also only takes keys
 * that have more than ten values to have meaningful trends. It further 
 * reduces the mapped output by taking data where the minimum years of 
 * schooling is less than 7 or where the maximum number of schooling is
 * greater than 18. This was done to have a wide data set but to also 
 * cut down on data from middle of the pack.
 * 
 * In post-processing, a python script was used to match data on country 
 * and year for (x, y) pairs where x would be the number of years schooled
 * and y is the fertility rate. 
 * 
 * 
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q5P {
	public static void main(String [] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"Q5");
		j.setJarByClass(Q5P.class);
		j.setMapperClass(MapForQ5.class);
		j.setReducerClass(ReduceForQ5.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	public static class MapForQ5 extends Mapper<LongWritable, Text, Text, Text>{
		static int lineCount = 0;
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String csv = value.toString();			
			String[] lines=csv.split("\",\n");
			
			
			for(String line: lines ){
				if(lineCount < 28262){
					lineCount++;
					continue;
				}else{
					String[] columns=line.split("\",\"");
					int columnsLength = columns.length;
					String code = columns[3];
					if(code.matches("SE.SCH.LIFE.FE|SP.DYN.TFRT.IN")){
						for(int i = 1; i<=30; i++){
							try{
								String stringValue = columns[columnsLength-i].replace("\"", "").replace(",", "");
								float floatValue = Float.valueOf(stringValue);
								String year = Integer.toString(2017-i);
								String country = columns[0].replace("\"", "");
								String keyString = country+"/"+columns[3];
								Text outputKey = new Text(keyString);
								Text outputValue = new Text(year+":"+stringValue);
								con.write(outputKey, outputValue);
							}catch(NumberFormatException e){
								continue;
							}
						}
					}
				}
			}
		}
	}
	public static class ReduceForQ5 extends Reducer<Text, Text, Text, Text> {
	    public void reduce(Text key, Iterable<Text> values, Context con)
	            throws IOException, InterruptedException {
	        
	    	int count = 0;
	    	float min = 15;
	    	float max = 0;
	        String myValue = "";
	        String code = key.toString().split("/")[1];
	        for (Text val : values) {
	        	count++;
	        	String[] yearValue = val.toString().split(":");
	        	float floatVal = Float.valueOf(yearValue[1]);
	        	if(floatVal < min && code.equals("SE.SCH.LIFE.FE")){
	        		min = floatVal;
	        	}
	        	if(floatVal > max && code.equals("SE.SCH.LIFE.FE")){
	        		max = floatVal;
	        	}
	        	myValue += val+",";
	        }
	        if(count >= 10 && ( (min < 7 || max > 18) || code.equals("SP.DYN.TFRT.IN") )){
		        Text newValue = new Text(myValue);
		        con.write(key, newValue);
	        }
	    }
	}
}
