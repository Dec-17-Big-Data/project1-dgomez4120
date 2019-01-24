
import org.junit.Test;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import Project_1.Q3P;

public class Q3Test {
    MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
    
    @Before
    public void setUp(){
    	Q3P.MapForQ3 mapper = new Q3P.MapForQ3();
    	mapDriver = MapDriver.newMapDriver(mapper);
    }
    
    @Test
    public void testMapper() throws IOException{
    	String REC = "\"United States\",\"USA\",\"Labor force participation rate, male (% of male population ages 15+) (national estimate)\",\"SL.TLF.CACT.MA.NE.ZS\",\"83.3399963378906\",\"82.8899993896484\",\"82\",\"81.370002746582\",\"81.0199966430664\",\"80.7200012207031\",\"80.4300003051758\",\"80.4300003051758\",\"80.0899963378906\",\"79.8499984741211\",\"79.6699981689453\",\"79.129997253418\",\"78.9499969482422\",\"78.8300018310547\",\"78.7200012207031\",\"77.879997253418\",\"77.5100021362305\",\"77.6600036621094\",\"77.8600006103516\",\"77.8300018310547\",\"77.4000015258789\",\"76.9800033569336\",\"76.5999984741211\",\"76.3899993896484\",\"76.3499984741211\",\"76.25\",\"76.25\",\"76.1900024414063\",\"76.1800003051758\",\"76.4300003051758\",\"76.3600006103516\",\"75.7799987792969\",\"75.8300018310547\",\"75.4300003051758\",\"75.0500030517578\",\"74.9800033569336\",\"74.9300003051758\",\"74.9700012207031\",\"74.8899993896484\",\"74.7200012207031\",\"74.8099975585938\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"69.1900024414063\",\n"+
    			"\"United States\",\"USA\",\"Labor force with advanced education, female (% of female working-age population with advanced education)\",\"SL.TLF.ADVN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\n"+
    			"\"United States\",\"USA\",\"Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate)\",\"SL.TLF.CACT.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.3030014038086\",\"74.6279983520508\",\"74.6780014038086\",\"74.3629989624023\",\"74.1149978637695\",\"74.1480026245117\",\"74.1699981689453\",\"74.2570037841797\",\"74.2649993896484\",\"74.2170028686523\",\"74.177001953125\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"68.2839965820313\",\n"+
    			"\"United Kingdom\",\"GBR\",\"Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate)\",\"SL.TLF.CACT.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"74.6569976806641\",\"74.004997253418\",\"72.7460021972656\",\"71.7050018310547\",\"71.4219970703125\",\"71.1050033569336\",\"70.8519973754883\",\"70.6240005493164\",\"70.2050018310547\",\"70.4039993286133\",\"70.390998840332\",\"69.6470031738281\",\"69.4929962158203\",\"69.8099975585938\",\"69.3710021972656\",\"69.2649993896484\",\"69.6340026855469\",\"69.5690002441406\",\"69.7369995117188\",\"69.2959976196289\",\"68.7050018310547\",\"68.6520004272461\",\"68.8310012817383\",\"68.7639999389648\",\"68.8199996948242\",\"68.713996887207\",\"68.6190032958984\",\n";
    	
    	mapDriver.withInput(new LongWritable(), new Text(REC));
    	
    	mapDriver.withOutput(new Text("United States SL.TLF.CACT.MA.NE.ZS 2016"),new FloatWritable(69.19f));
    	mapDriver.withOutput(new Text("United States SL.TLF.CACT.MA.NE.ZS 2000"),new FloatWritable(74.81f));
    	mapDriver.withOutput(new Text("United States SL.TLF.CACT.MA.ZS 2016"),new FloatWritable(68.284f));
    	mapDriver.withOutput(new Text("United States SL.TLF.CACT.MA.ZS 2000"),new FloatWritable(74.177f));
    	
    	mapDriver.runTest(false);
    }
}
