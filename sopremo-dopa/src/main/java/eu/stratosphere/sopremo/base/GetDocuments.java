package eu.stratosphere.sopremo.base;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.GenericSopremoMap;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecord;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;



@Name(verb = "getDocuments")
@InputCardinality(1)
public class GetDocuments extends ElementaryOperator<GetDocuments> {
	
	protected static final String PERAMETER_VALUE = "ser_parameter";
	private IJsonNode parameterValue= null;
  
	
    
	@SuppressWarnings("serial")
	public static class Implementation extends GenericSopremoMap<IJsonNode,IJsonNode> implements InputFormat<SopremoRecord, GenericInputSplit> {
		
		Configuration conf = HBaseConfiguration.create();
		
		PactString data_pool = new PactString();
		PactString identifier = new PactString();
		PactString meta_data = new PactString();
		
		DataMarketAccess dm = new DataMarketAccess();
		PactString content = new PactString();
		
		PactRecord out = new PactRecord();
		
		//new parameter to configure input parameter
		private EvaluationContext context;
		private String parameter;
		
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
				getHbaseContent(url, crawlId, obj);
				out.collect(obj);
			//TODO handle the incoming parameter	
			}
		}
		
		
		/**
		 * Perform an HBase get for row 'url' on table 'crawlId' 
		 * 
		 * @param url : String the url referencing the HBASE row
		 * @param crawlId : String the crawlId referencing the HBase table
		 * @param value : IObjectNode the parsed input Json object to enrich with hbase content
		 */
		private void getHbaseContent(String url, String crawlId, IObjectNode value){
			
			if (crawlId != null && !crawlId.matches("")){
				conf.addResource(new Path("file:///0/platform-strato/hbase-site_imr.xml"));
				
				HTable table;
				try {
					table = new HTable(conf, crawlId);
				
					
				//the "rows" are the urls 
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
		        // crawlId already contained in the object
		        /*if(value4 != null) {
		        	String crawl = new String (value4, Charset.forName("UTF-8"));
		        }*/

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
		}


		@Override
		public void configure(
				eu.stratosphere.nephele.configuration.Configuration parameters) {
			
			this.context = SopremoUtil.getEvaluationContext(parameters);
            SopremoEnvironment.getInstance().setEvaluationContext(context);
			parameter = parameters.getString(PERAMETER_VALUE, null);
			
		}


		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public GenericInputSplit[] createInputSplits(int minNumSplits)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public Class<? extends GenericInputSplit> getInputSplitType() {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public void open(GenericInputSplit split) throws IOException {
			// TODO Auto-generated method stub
			
		}


		@Override
		public boolean reachedEnd() throws IOException {
			// TODO Auto-generated method stub
			return false;
		}


		@Override
		public boolean nextRecord(SopremoRecord record) throws IOException {
			// TODO Auto-generated method stub
			return false;
		}
		
		@Override
	        public void close() throws IOException {
	            // TODO Auto-generated method stub
	        }
	}
		
		
	
	// unchanged method from super-class
	//TODO: add parameter for data pool
	//TODO: add parameter for handling of input with missing information
	public PactModule asPactModule(EvaluationContext context) {

		GenericDataSource<?> contract = new GenericDataSource<Implementation>(
				Implementation.class, String.format("GetDocuments %s",parameterValue.toString()));
		SopremoUtil.setEvaluationContext(contract.getParameters(), context);
		
		//PacgModel(int numberOfInputs,int numberOfOutputs)
		PactModule module = new PactModule (1, 1);
		
/*		MapContract.Builder builder = MapContract.builder(Implementation.class);
		builder.name(this.toString());
		builder.input(module.getInput(0));
		MapContract mapcontract = builder.build();	
		SopremoUtil.serialize(reducecontract.getParameters(), FIRST_VALUE, firstValue);
*/
		contract.getParameters().setString(PERAMETER_VALUE, parameterValue.toString());
		module.getOutput(0).setInput(contract);
		return module;
	}
	
	
	@Property(preferred = true)
	@Name(preposition = "for")
	public void setParameterValue(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		this.parameterValue = value.evaluate(NullNode.getInstance());
		
		System.out.println("set parameter expression " + parameterValue.toString());
	}
	
	
}

