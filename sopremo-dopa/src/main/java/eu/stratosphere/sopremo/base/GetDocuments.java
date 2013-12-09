package eu.stratosphere.sopremo.base;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.ContractUtil;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.util.IdentityList;

public class GetDocuments extends ElementaryOperator<GetDocuments> {

	public static class Implementation extends MapStub {
		
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
		public void map(PactRecord record, Collector<PactRecord> out)
				throws Exception {
			// only for data pool = IMR
			
			//TODO: determine if record is a single item or a list
			for(int k=0; k<record.getNumFields();k++){
				
				PactString url = record.getField(0, PactString.class);			
				out.collect(buildPactRecord(url));
			}
			

		}
		
		
		private PactRecord buildPactRecord(PactString url){
			
		
			
			conf.addResource(new Path("file:///0/platform-strato/hbase-site_imr.xml"));
			
			//TODO: find out what the tablename is
			HTable table = new HTable(conf, tablename);
				
				//the "row's" are the url's 
				byte[] row = url.toString().getBytes();
				
	            Get get = new Get(row);
	            get.addColumn(BASELINE_FAMILY, TITLE_QUALIFIER);
	            get.addColumn(BASELINE_FAMILY, TEXT_QUALIFIER);
	            get.addColumn(META_FAMILY, LANGUAGE_QUALIFIER);
	            get.addColumn(META_FAMILY, MIME_QUALIFIER);
	            get.addColumn(META_FAMILY, CRAWLID_QUALIFIER);
	            
	            //get the information/results from the hBase table
	            Result res = table.get(get);
	          
	            //extract all the data and put it in an object
	            byte[] value = res.getRow();
	
	            // convert to String
	            String content = new String (value, Charset.forName("UTF-8"));
	
	            
	            //TODO handle content
	            
	            
	            
	            PactRecord out = new PactRecord();
	            
	            // TODO build PactRecord
	            
	            
	            out.setField(0, this.identifier);
				out.setField(1, this.content);
				
				return out;
			
		}
		
	}
		
		
	
	// unchanged method from super-class
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
	
}

