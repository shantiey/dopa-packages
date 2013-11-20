package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.DataMarketAccess.DataMarketInputFormat;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;

@Name(verb = "getDocuments")
@InputCardinality(0)
public class GetDocuments extends ElementaryOperator<GetDocuments> {
	
	protected static final String DATA_POOL = "data_pool";
	protected static final String DOCUMENT_IDENTIFIERS = "document_identifiers";
	
	private String data_pool=null;
	private String document_identifiers=null;
	
	
	@Override
	public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
		
		//FIXME: build suitable contract for getDocuments	
		GenericDataSource<?> contract = new GenericDataSource<DataMarketInputFormat>(
				DataMarketInputFormat.class, String.format("DataMarket %s", urlParameterNodeString));
	
		final PactModule pactModule = new PactModule(0, 1);
		SopremoUtil.setEvaluationContext(contract.getParameters(), context);
	    SopremoUtil.setLayout(contract.getParameters(), layout);
	    contract.getParameters().setString(DATA_POOL, data_pool);
	    contract.getParameters().setString(DOCUMENT_IDENTIFIERS, document_identifiers);
		pactModule.getOutput(0).setInput(contract);
		return pactModule;
	}
	
	@Property(preferred = true)
	@Name(noun = "for")
	public void setDocumentIdentifiers(EvaluationExpression value) {
		//TODO: evaluate given json file for document identifiers
	}
	
	@Property(preferred = true)
	@Name(noun = "on")
	public void setDataPool(EvaluationExpression value) {
		//TODO: evaluate given value for data pool enum
	}
	
	@Property(preferred = false)
	@Name(noun = "with")
	public void setAdditionalMetaData(EvaluationExpression value) {
		//TODO: evaluate given json file for additional metadata
	}

}
