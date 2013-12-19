package eu.stratosphere.sopremo.base;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.DataMarketAccess.DataMarketInputFormat;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.GenericSopremoMap;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.util.IdentityList;



@Name(verb = "getDocuments")
@InputCardinality(1)
public class GetDocuments extends ElementaryOperator<GetDocuments> {
	
	boolean keepUnfound = true;
	
	public boolean getKeepUnfound(){
		return this.keepUnfound;
	}
	
	public static class Implementation extends GenericSopremoMap<IJsonNode,IJsonNode> {
		
		Configuration conf = HBaseConfiguration.create();
		
		boolean keepUnfound = ;
		
		PactString data_pool = new PactString();
		PactString identifier = new PactString();
		PactString meta_data = new PactString();
		
		//DataMarketAccess dm = new DataMarketAccess();
		PactString content = new PactString();
		
		PactRecord out = new PactRecord();
		
		// baseline family
	    private static final byte[] BASELINE_FAMILY = "baseline".getBytes();
	    private static final byte[] TITLE_QUALIFIER = "title".getBytes();
	    private static final byte[] TEXT_QUALIFIER = "textContent".getBytes();

	    // meta family
	    private static final byte[] META_FAMILY = "meta".getBytes();
	    private static final byte[] LANGUAGE_QUALIFIER = "language".getBytes();
	    private static final byte[] MIME_QUALIFIER = "mime".getBytes();
	    private static final byte[] CRAWLID_QUALIFIER = "crawlId".getBytes();


	    @Override
	    protected void map(IJsonNode value, JsonCollector<IJsonNode> out) {
			// only for data pool = IMR
			String url;
			String crawlId;
			if (value instanceof IObjectNode) {
				IObjectNode obj = (IObjectNode) value;
				IJsonNode jsonurl = obj.get("url");
				url = jsonurl.toString();
				IJsonNode jsoncrawlId = obj.get("crawlId");
				crawlId = jsoncrawlId.toString();
				
				boolean succ = getHbaseContent(url, crawlId, obj);
				if( && succ){
					out.collect(obj);
				}
				
			}
		}
		
		
		/**
		 * Perform an HBase get for row 'url' on table 'crawlId' 
		 * 
		 * @param url : String the url referencing the HBASE row
		 * @param crawlId : String the crawlId referencing the HBase table
		 * @param value : IObjectNode the parsed input Json object to enrich with hbase content
		 */
		private boolean getHbaseContent(String url, String crawlId, IObjectNode value){
			
			boolean complete = true;
			
			if (crawlId != null && !crawlId.matches("")){
				conf.addResource(new Path("file:///0/platform-strato/hbase-site_imr.xml"));
				
				HTable table;
				try {
					table = new HTable(conf, crawlId);
				
					
				if(url != null && !url.matches("")){
					byte[] row = url.getBytes();
					
		            Get get = new Get(row);
			        get.addColumn(BASELINE_FAMILY, TITLE_QUALIFIER);
			        get.addColumn(BASELINE_FAMILY, TEXT_QUALIFIER);
		            get.addColumn(META_FAMILY, LANGUAGE_QUALIFIER);
		            get.addColumn(META_FAMILY, MIME_QUALIFIER);
			        get.addColumn(META_FAMILY, CRAWLID_QUALIFIER);
			            
			        //get the information/results from the HBase table
			        Result res = table.get(get);
			        table.close();
					
			          
			        //extract all the data and put it in an object
			        byte[] value0 = res.getValue(BASELINE_FAMILY, TITLE_QUALIFIER);
			        byte[] value1 = res.getValue(BASELINE_FAMILY, TEXT_QUALIFIER);
			        byte[] value2 = res.getValue(META_FAMILY, LANGUAGE_QUALIFIER);
			        byte[] value3 = res.getValue(META_FAMILY, MIME_QUALIFIER);
			        //byte[] value4 = res.getValue(META_FAMILY, CRAWLID_QUALIFIER);
			
			        // convert to String
			        if(value0 != null) {
			        	String title = new String (value0, Charset.forName("UTF-8"));
			        	value.put("title", new TextNode (title));
			        }
			        if(value1 != null) {
			        	String text = new String (value1, Charset.forName("UTF-8"));
			        	value.put("text", new TextNode (text));
			        }
			        if(value2 != null) {
			        	String language = new String (value2, Charset.forName("UTF-8"));
			        	value.put("language", new TextNode (language));
			        }
			        if(value3 != null) {
			        	String mime = new String (value3, Charset.forName("UTF-8"));
			        	value.put("mime", new TextNode (mime));
			        }
				} else {
					complete = false;
				}
				
		        // crawlId already contained in the object
		        /*if(value4 != null) {
		        	String crawl = new String (value4, Charset.forName("UTF-8"));
		        }*/

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			} else {
				complete = false;
			}
			return complete;
		}
	}
	
	@Property(preferred = false)
	@Name(noun = "keepUnfound")
	public void setKeepUnfound(EvaluationExpression value) {
		boolean res = true;
		if (value != null) {
			IJsonNode node = value.evaluate(NullNode.getInstance());
			if (node.toString().toLowerCase().matches("false")) {
				res = false;
			}
		}
		
		this.keepUnfound = res;
	}
		
		
	
	// copied from DataMarketAccess
	//TODO: add parameter for data pool
	//TODO: add parameter for handling of input with missing information
	@Override
	public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
		GenericDataSource<?> contract = new GenericDataSource<DataMarketInputFormat>(
				DataMarketInputFormat.class, String.format("DataMarket %s", urlParameterNodeString));

		final PactModule pactModule = new PactModule(0, 1);
        SopremoUtil.setEvaluationContext(contract.getParameters(), context);
        SopremoUtil.setLayout(contract.getParameters(), layout);
        contract.getParameters().setString(DM_URL_PARAMETER, urlParameterNodeString);
        contract.getParameters().setString(DM_API_KEY_PARAMETER, dmApiKeyString);
        contract.getParameters().setString(DM_MAX_DATE, maxdate);
        contract.getParameters().setString(DM_MIN_DATE, mindate);
		pactModule.getOutput(0).setInput(contract);
		return pactModule;
	}
	
}

