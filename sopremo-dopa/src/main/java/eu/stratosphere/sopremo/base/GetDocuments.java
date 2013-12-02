package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;

public class GetDocuments extends ElementaryOperator<GetDocuments> {
	
	

	public static class Implementation extends MapStub {
		
		PactString data_pool = new PactString();
		PactString identifier = new PactString();
		PactString meta_data = new PactString();
		
		DataMarketAccess dm = new DataMarketAccess();
		PactString content = new PactString();
		
		PactRecord out = new PactRecord();

		@Override
		public void map(PactRecord record, Collector<PactRecord> out)
				throws Exception {
			this.identifier = record.getField(0, PactString.class);
			this.data_pool = record.getField(1, PactString.class);
			this.meta_data = record.getField(2, PactString.class);

			if(this.data_pool.toString().matches("DM")) {
				//TODO: get content from DM
				dm.setURLParameter(new ConstantExpression(this.identifier.toString()));


			} else if (this.data_pool.toString().matches("IMR")) {
				//TODO: get content from IMR
			}

			this.out.setField(0, this.identifier);
			this.out.setField(1, this.content);

		}

	}
}
