package eu.stratosphere.sopremo.base;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecord;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.NullNode;

@Name(verb = "getDocuments")
@InputCardinality(0)
public class GetDocuments extends ElementaryOperator<GetDocuments> {
	
	protected static final String DATA_POOL = "data_pool";
	protected static final String DOCUMENT_IDENTIFIERS = "document_identifiers";
	
	private String data_pool=null;
	private String document_identifiers=null;
	
	
	public static class GetDocumentsInputFormat implements InputFormat<SopremoRecord, GenericInputSplit> {

		private static final long serialVersionUID = 3885278940930026634L;
		
		private EvaluationContext context;

		@Override
		public void configure(Configuration parameters) {
			this.context = SopremoUtil.getEvaluationContext(parameters);
            SopremoEnvironment.getInstance().setEvaluationContext(context);
			// TODO Auto-generated method stub
			
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
	
	@Override
	public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
		
		//FIXME: build suitable contract for getDocuments	
		GenericDataSource<?> contract = new GenericDataSource<GetDocumentsInputFormat>(
				GetDocumentsInputFormat.class, String.format("GetDocuments %s", document_identifiers));
	
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
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		document_identifiers = value.evaluate(NullNode.getInstance()).toString();
		// does this work? how does this string look?
	}
	
	@Property(preferred = true)
	@Name(noun = "on")
	public void setDataPool(EvaluationExpression value) {
		if (value == null) {
			throw new NullPointerException("value expression must not be null");
		} else {
			if (value.toString() == "DM" || value.toString() == "IMR") {
				data_pool = value.toString();
			} else {
				//TODO: throw some exception for wrong expression usage (after 'on' must follow 'DM' or 'IMR')
			}
		}
	}
	
	@Property(preferred = false)
	@Name(noun = "with")
	public void setAdditionalMetaData(EvaluationExpression value) {
		//TODO: evaluate given json file for additional metadata
	}

}
