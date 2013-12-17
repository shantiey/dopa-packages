package eu.stratosphere.sopremo.base;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.pact.GenericSopremoMap;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.TextNode;



@Name(verb = "getDocuments")
@InputCardinality(1)
public class GetDocuments extends ElementaryOperator<GetDocuments> {
	
	public static class Implementation extends GenericSopremoMap<IJsonNode,IJsonNode> {
		
		Configuration conf = HBaseConfiguration.create();
		
		PactString data_pool = new PactString();
		PactString identifier = new PactString();
		PactString meta_data = new PactString();
		
		DataMarketAccess dm = new DataMarketAccess();
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
				getHbaseContent(url, crawlId, obj);
				out.collect(obj);
			}

		
		}
		
		
		/**
		 * Perform an HBase get for row 'url' on table 'crawlId' 
		 * 
		 * @param url the url referencing the HBASE row
		 * @param crawlId the crawlId referencing the HBase table
		 * @return return a PactRecord containing the retrieved content
		 */
		private void getHbaseContent(String url, String crawlId, IObjectNode value){
			
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
	        byte[] value4 = res.getValue(META_FAMILY, CRAWLID_QUALIFIER);
	
	        // convert to String
            String title = new String (value0, Charset.forName("UTF-8"));
            String text = new String (value1, Charset.forName("UTF-8"));
	        String language = new String (value2, Charset.forName("UTF-8"));
	        String mime = new String (value3, Charset.forName("UTF-8"));	
	        String crawl = new String (value4, Charset.forName("UTF-8"));
	        
	       
	        value.put("title", new TextNode (title));
	        value.put("text", new TextNode (text));
	        value.put("language", new TextNode (language));
	        value.put("mime", new TextNode (mime));

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			
		}
	}
		
		
	
/*	// unchanged method from super-class
	//TODO: add parameter for data pool
	@Override
	public PactModule asPactModule(final EvaluationContext context, SopremoRecordLayout layout) {
		final Contract contract = this.getContract(layout);
		context.setResultProjection(this.resultProjection);
		this.configureContract(contract, contract.getParameters(), context, layout);

		final List<List<Contract>> inputLists = ContractUtil
			.getInputs(contract);
		final List<Contract> distinctInputs = new IdentityList<Contract>();
		for (final List<Contract> inputs : inputLists) {
			// assume at least one input for each contract input slot
			if (inputs.isEmpty())
				inputs.add(MapContract.builder(IdentityMap.class).build());
			for (final Contract input : inputs)
				if (!distinctInputs.contains(input))
					distinctInputs.add(input);
		}
		final PactModule module = new PactModule(distinctInputs.size(), 1);
		for (final List<Contract> inputs : inputLists)
			for (int index = 0; index < inputs.size(); index++)
				inputs.set(index, module.getInput(distinctInputs.indexOf(inputs.get(index))));
		ContractUtil.setInputs(contract, inputLists);

		module.getOutput(0).addInput(contract);
		return module;
	}
	*/
}

