
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import Project_1.Q1P;


public class Q1Test {
    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable,Text,Text,DoubleWritable,Text,DoubleWritable> mapReduceDriver;
    
    @Before
    public void setUp(){
    	Q1P.MapForQ1 mapper = new Q1P.MapForQ1();
    	Q1P.ReduceForQ1 reducer = new Q1P.ReduceForQ1();
    	mapDriver = MapDriver.newMapDriver(mapper);
    	reduceDriver = ReduceDriver.newReduceDriver(reducer);
    	mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    
    @Test
    public void testMapper() throws IOException{
    	String PRM = "\"Chad\",\"TCD\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.89788\",\"\",\"\",\"\",\"15.56095\",\"16.84211\",\"18.44881\",\"15.71024\",\"\",\"\",\"\",\n"+
    			"\"Malawi\",\"MWI\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"29.51234\",\"\",\n" +
    			"\"Malaysia\",\"MYS\",\"Gross graduation ratio, primary, male (%)\",\"SE.PRM.CMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"101.07862\",\"100.28091\",\"99.33497\",\"\",\n" +
    			"\"Montenegro\",\"MNE\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"95.14867\",\"\",\n"+
    			"\"Belize\",\"BLZ\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.89261\",\"16.43875\",\"\",\"\",\"9.56951\",\"\",\n"+
    			"\"Brazil\",\"BRA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.50004\",\"19.28205\",\"21.4784\",\"24.2811\",\"\",\"25.6837\",\"28.08802\",\"31.5008\",\"\",\"31.82378\",\"34.59072\",\"\",\"\",\"\",\"\",\n"+
    			"\"Argentina\",\"ARG\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10.29958\",\"8.60549\",\"13.46936\",\"14.3721\",\"\",\"15.76742\",\"13.72042\",\"14.12948\",\"14.65367\",\"\",\"\",\"14.09986\",\"13.82683\",\"13.71856\",\"\",\"\",\n";
    	
    	mapDriver.withInput(new LongWritable(), new Text(PRM));
    	
    	
    	mapDriver.withOutput(new Text("SE.PRM.CMPL.FE.ZS Chad 2013"),new DoubleWritable(15.71024));
    	mapDriver.withOutput(new Text("SE.PRM.CMPL.FE.ZS Malawi 2015"),new DoubleWritable(29.51234));
    	mapDriver.withOutput(new Text("SE.TER.CMPL.FE.ZS Belize 2015"), new DoubleWritable(9.56951));
    	mapDriver.withOutput(new Text("SE.TER.CMPL.FE.ZS Argentina 2014"), new DoubleWritable(13.71856));
    
    	mapDriver.runTest();
    }
    
    @Test
    public void testReducer() throws IOException{
    	List<DoubleWritable> values1 = new ArrayList<DoubleWritable>();
		values1.add(new DoubleWritable(15.71024));
    	reduceDriver.withInput(new Text("SE.PRM.CMPL.FE.ZS Chad 2013"), values1);
    	
    	List<DoubleWritable> values2 = new ArrayList<DoubleWritable>();
		values2.add(new DoubleWritable(29.51234));
    	reduceDriver.withInput(new Text("SE.PRM.CMPL.FE.ZS Malawi 2015"), values2);
    	
    	List<DoubleWritable> values3 = new ArrayList<DoubleWritable>();
		values3.add(new DoubleWritable(13.71856));
    	reduceDriver.withInput(new Text("SE.TER.CMPL.FE.ZS Argentina 2014"), values3);
    	
    	List<DoubleWritable> values4 = new ArrayList<DoubleWritable>();
		values4.add(new DoubleWritable(9.56951));
    	reduceDriver.withInput(new Text("SE.TER.CMPL.FE.ZS Belize 2015"), values4);
    	
    	reduceDriver.withOutput(new Text("SE.PRM.CMPL.FE.ZS Chad 2013"),new DoubleWritable(15.71024));
    	reduceDriver.withOutput(new Text("SE.PRM.CMPL.FE.ZS Malawi 2015"),new DoubleWritable(29.51234));
    	reduceDriver.withOutput(new Text("SE.TER.CMPL.FE.ZS Argentina 2014"), new DoubleWritable(13.71856));
    	reduceDriver.withOutput(new Text("SE.TER.CMPL.FE.ZS Belize 2015"), new DoubleWritable(9.56951));
    
    	reduceDriver.run();
    }
    
    @Test
    public void testMapReducer(){
    	String PRM = "\"Chad\",\"TCD\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.89788\",\"\",\"\",\"\",\"15.56095\",\"16.84211\",\"18.44881\",\"15.71024\",\"\",\"\",\"\",\n"+
    			"\"Malawi\",\"MWI\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"29.51234\",\"\",\n" +
    			"\"Malaysia\",\"MYS\",\"Gross graduation ratio, primary, male (%)\",\"SE.PRM.CMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"101.07862\",\"100.28091\",\"99.33497\",\"\",\n" +
    			"\"Montenegro\",\"MNE\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"95.14867\",\"\",\n";
    	String TER = "\"Belize\",\"BLZ\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.89261\",\"16.43875\",\"\",\"\",\"9.56951\",\"\",\n"+
    			"\"Brazil\",\"BRA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.50004\",\"19.28205\",\"21.4784\",\"24.2811\",\"\",\"25.6837\",\"28.08802\",\"31.5008\",\"\",\"31.82378\",\"34.59072\",\"\",\"\",\"\",\"\",\n"+
    			"\"Argentina\",\"ARG\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10.29958\",\"8.60549\",\"13.46936\",\"14.3721\",\"\",\"15.76742\",\"13.72042\",\"14.12948\",\"14.65367\",\"\",\"\",\"14.09986\",\"13.82683\",\"13.71856\",\"\",\"\",\n";
    	
    	mapReduceDriver.withInput(new LongWritable(), new Text(PRM));
    	mapReduceDriver.withInput(new LongWritable(), new Text(TER));
    	
    	mapReduceDriver.withOutput(new Text("SE.PRM.CMPL.FE.ZS Chad 2013"),new DoubleWritable(15.71024));
    	mapReduceDriver.withOutput(new Text("SE.PRM.CMPL.FE.ZS Malawi 2015"),new DoubleWritable(29.51234));
    	mapReduceDriver.withOutput(new Text("SE.TER.CMPL.FE.ZS Argentina 2014"), new DoubleWritable(13.71856));
    	mapReduceDriver.withOutput(new Text("SE.TER.CMPL.FE.ZS Belize 2015"), new DoubleWritable(9.56951));
    
    	mapReduceDriver.runTest();
    }
}
