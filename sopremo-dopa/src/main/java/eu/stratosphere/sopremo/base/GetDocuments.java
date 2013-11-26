package eu.stratosphere.sopremo.base;

import java.io.IOException;
import java.util.Iterator;

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
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
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
		private String identifier;
		private String pool;
		
		private Iterator<IJsonNode> nodeIterator;

		@Override
		public void configure(Configuration parameters) {
			this.context = SopremoUtil.getEvaluationContext(parameters);
            SopremoEnvironment.getInstance().setEvaluationContext(context);
            identifier = parameters.getString(DOCUMENT_IDENTIFIERS, null);
            pool = parameters.getString(DATA_POOL, null);
		}
		
		private String getDMDocument(String identifier) {
			// TODO: implement actual document retrieval (DataMarket)
			DataMarketAccess dmAccess = new DataMarketAccess();
			return null;
		}
		
		private String getIMRDocument(String identifier) {
			// TODO: implement actual document retrieval (IMR)
			return null;
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericInputSplit[] createInputSplits(int minNumSplits) throws IOException {
			GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                splits[i] = new GenericInputSplit(i);
            }
            return splits;
		}

		@Override
		public Class<? extends GenericInputSplit> getInputSplitType() {
			return GenericInputSplit.class;
		}

		@Override
		public void open(GenericInputSplit split) throws IOException {
			if (split.getSplitNumber() == 0) {
				
				String response = null;
				if (this.pool.matches("DM")) {
					response = getDMDocument(this.identifier);
				} else if (this.pool.matches("IMR")) {
					response = getIMRDocument(this.identifier);
				}
				ArrayNode<IJsonNode> records = convertRespone(response);
				
				this.nodeIterator = records.iterator();
				
			} else {
				this.nodeIterator = null;
			}
			
		}

		private ArrayNode<IJsonNode> convertRespone(String response) {
			// TODO convert the String responses to IJsonNodes
			return null;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (nodeIterator == null) {
				return true;
			}
			return !this.nodeIterator.hasNext();
		}

		@Override
		public boolean nextRecord(SopremoRecord record) throws IOException {
			if (this.reachedEnd())
                throw new IOException("End of input split is reached");

            final IJsonNode value = this.nodeIterator.next();
            record.setNode(value);
            return true;
		}

		@Override
		public void close() throws IOException {
			// do nothing
		}
		
	}
	
	@Override
	public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
		
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
			if (value.toString().matches("DM") || value.toString().matches("IMR")) {
				data_pool = value.toString();
			} else {
				//TODO: throw some exception for wrong expression usage (after 'on' must follow 'DM' or 'IMR')
			}
		}
	}
	
	@Property(preferred = false)
	@Name(noun = "with")
	public void setAdditionalMetaData(EvaluationExpression value) {
		//TODO: evaluate committed json file for additional metadata
	}

}
