
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import Project_1.Q2P;


public class Q2Test {
    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    
    @Before
    public void setUp(){
    	Q2P.MapForQ2 mapper = new Q2P.MapForQ2();
    	mapDriver = MapDriver.newMapDriver(mapper);
    }
    
    @Test
    public void testMapper() throws IOException{
    	String REC = "\"United States\",\"USA\",\"School enrollment, primary, female (% gross)\",\"SE.PRM.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"99.81719\",\"101.36085\",\"99.05254\",\"98.58923\",\"99.17071\",\"100.49677\",\"100.73134\",\"\",\"\",\"105.1642\",\"103.67622\",\"\",\"101.97208\",\"101.96344\",\"103.37849\",\"103.36004\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"100.03818\",\"\",\n"+
    			"\"United States\",\"USA\",\"School enrollment, primary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"97.07956\",\"95.69427\",\"95.43967\",\"94.11045\",\"94.42654\",\"95.79755\",\"96.439\",\"96.82568\",\"95.78414\",\"93.85194\",\"93.59352\",\"92.98951\",\"92.62706\",\"93.32211\",\"94.0818\",\"\",\n"+
    			"\"United States\",\"USA\",\"School enrollment, secondary, female (% gross)\",\"SE.SEC.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"60.67222\",\"\",\"\",\"\",\"\",\"\",\"85.3929\",\"\",\"\",\"91.41668\",\"94.67477\",\"94.75381\",\"96.46224\",\"95.0773\",\"96.60813\",\"96.64305\",\"\",\"\",\"91.47174\",\"92.25663\",\"\",\"95.98984\",\"96.81125\",\"96.40682\",\"95.25981\",\"\",\"\",\"\",\"93.88283\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"98.47282\",\"\",\"\",\n"+
    			"\"United States\",\"USA\",\"School enrollment, tertiary, female (% gross)\",\"SE.TER.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"38.56622\",\"40.2428\",\"42.38329\",\"43.68353\",\"46.55416\",\"49.95025\",\"50.68366\",\"52.72272\",\"53.23715\",\"55.32357\",\"58.25927\",\"59.95962\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"99.59588\",\"\",\n"+
    			"\"Uruguay\",\"URY\",\"School enrollment, tertiary, female (% gross)\",\"SE.TER.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.30792\",\"13.44676\",\"\",\"16.79387\",\"\",\"17.05721\",\"16.35839\",\"\",\"22.40141\",\"23.86031\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.88219\",\"43.12973\",\"43.96197\",\"48.62929\",\"52.13518\",\"55.43754\",\"56.98448\",\"57.86762\",\"57.93456\",\"81.48148\",\"82.479\",\"80.60512\",\"80.3234\",\"\",\"\",\"\",\"\",\"\",\"\",\n"+
    			"\"Uzbekistan\",\"UZB\",\"School enrollment, secondary, female (% gross)\",\"SE.SEC.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"84.71611\",\"84.97315\",\"\",\"\",\"94.94043\",\"95.81789\",\"88.26811\",\"89.30335\",\"91.95339\",\"91.68919\",\"94.12406\",\"94.79775\",\"94.9411\",\"96.77186\",\"95.95387\",\"95.5688\",\"94.83303\",\"95.05953\",\n"+
    			"\"Vanuatu\",\"VUT\",\"School enrollment, primary, female (% gross)\",\"SE.PRM.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"112.0893\",\"116.75993\",\"112.25166\",\"112.1719\",\"111.44828\",\"110.74451\",\"110.74469\",\"110.54171\",\"111.58099\",\"113.02031\",\"114.58085\",\"110.82425\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"99.08359\",\"95.97534\",\"94.34772\",\"96.7026\",\"109.123\",\"108.27311\",\"\",\"\",\"110.39455\",\"118.60751\",\"116.54194\",\"118.63163\",\"119.97252\",\"121.15682\",\"124.68542\",\"119.97808\",\"116.14942\",\"110.74912\",\"111.1311\",\"113.84634\",\"112.8575\",\"121.88308\",\"\",\"\",\"122.20727\",\"\",\"\",\"115.1414\",\n";
    	
    	mapDriver.withInput(new LongWritable(), new Text(REC));
    	
    	mapDriver.withOutput(new Text("United States SE.PRM.ENRR.FE 2016"),new DoubleWritable(100.03818));
    	mapDriver.withOutput(new Text("United States SE.SEC.ENRR.FE 2016"),new DoubleWritable(98.47282));
    	mapDriver.withOutput(new Text("United States SE.SEC.ENRR.FE 2002"),new DoubleWritable(93.88283));
    	mapDriver.withOutput(new Text("United States SE.TER.ENRR.FE 2016"),new DoubleWritable(99.59588));
        
    	mapDriver.runTest(false);
    }
}
