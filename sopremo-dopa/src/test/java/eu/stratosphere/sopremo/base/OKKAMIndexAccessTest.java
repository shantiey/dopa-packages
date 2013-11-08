package eu.stratosphere.sopremo.base;

/**
 * Created with IntelliJ IDEA.
 * User: mleich
 * Date: 7/11/2013
 * Time: 14:21
 * To change this template use File | Settings | File Templates.
 */


import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import javolution.testing.JUnitContext;
import org.junit.Test;

public class OKKAMIndexAccessTest {

    @Test
    public void shouldAccessOKKAMIndex () {
        final SopremoTestPlan sopremoPlan = new eu.stratosphere.sopremo.testing.SopremoTestPlan(0, 1);

        OKKAMIndexAccess okkam = new OKKAMIndexAccess();

        String jsonQuery = "{\"datapool\":\"imr\",\"crawlid\":\"test\",\"sortBy\":\"timestamp\",\"queryOkkamIds\":\"okkam-content-entity:ens:eid-af200f1e-3485-49a0-b350-2d9291c66058 OR ens:eid-664c165c-86f5-4962-8024-6fc339d085d4\",\"queryNamedEntities\":\"content-entity-any:Age Custom Programs\",\"limit\":10,\"joinClause\":\"OR\"}";
        JsonParser parser = new JsonParser(jsonQuery);
        IJsonNode query = null;
        try {
            query = parser.readValueAsTree();
        } catch (JsonParseException e) {
            JUnitContext.fail();
        }

        okkam.setQueryParameter(new ConstantExpression(query));

        sopremoPlan.getOutputOperator(0).setInputs(okkam);
        sopremoPlan.run();
        SopremoTestPlan.ActualOutput out = sopremoPlan.getActualOutput(0);
        for (IJsonNode node : out) {
            System.out.println(node.toString());
        }
    }
}
