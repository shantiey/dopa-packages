package eu.stratosphere.sopremo.base;

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class DataMarketImportTest {
	/**
	 * copy from DataMarketAccessTest
	 * Date/Time:24.02.14
	 */
	public class DataMarketAccessTest {

	    @Test
	    public void shouldAccessDataMarket () {
	        final SopremoTestPlan sopremoPlan = new eu.stratosphere.sopremo.testing.SopremoTestPlan(0, 1);

	        DataMarketImport dm = new DataMarketImport();
	        //datamarketaccess for '[{"ds_id":"282e"}]' mindate '2011-01-01' maxdate '2012-05-31'  key 'd5e368122c8b41d6b0f431e508792780';
	        dm.setImportParameter(new ConstantExpression(new TextNode("[{\"Country\":\"de\"}]")));
	       
	//        dm.setImportParameter(new ConstantExpression(new TextNode ("d5e368122c8b41d6b0f431e508792780")));
	        sopremoPlan.getOutputOperator(0).setInputs(dm);
	        sopremoPlan.run();
	        SopremoTestPlan.ActualOutput out = sopremoPlan.getActualOutput(0);
	        for (IJsonNode node : out) {
	            System.out.println(node.toString());
	        }
	    }
	}	
}