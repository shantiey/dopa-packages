package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: mleich
 * Date: 7/11/2013
 * Time: 16:31
 * To change this template use File | Settings | File Templates.
 */
public class DataMarketAccessTest {

    @Test
    public void shouldAccessDataMarket () {
        final SopremoTestPlan sopremoPlan = new eu.stratosphere.sopremo.testing.SopremoTestPlan(0, 1);

        DataMarketAccess dm = new DataMarketAccess();
        //datamarketaccess for '[{"ds_id":"282e"}]' mindate '2011-01-01' maxdate '2012-05-31'  key 'd5e368122c8b41d6b0f431e508792780';
        dm.setURLParameter(new ConstantExpression(new TextNode("[{\"ds_id\":\"282e\"}]")));
        dm.setMinDate(new ConstantExpression(new TextNode ("2011-01-01")));
        dm.setMaxDate(new ConstantExpression(new TextNode ("2012-05-31")));
        dm.setKeyParameter(new ConstantExpression(new TextNode ("d5e368122c8b41d6b0f431e508792780")));
        sopremoPlan.getOutputOperator(0).setInputs(dm);
        sopremoPlan.run();
        SopremoTestPlan.ActualOutput out = sopremoPlan.getActualOutput(0);
        for (IJsonNode node : out) {
            System.out.println(node.toString());
        }
    }
}
