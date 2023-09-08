package zingg.spark.core.executor;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.SparkSession;

import zingg.common.client.Arguments;
import zingg.common.client.ZinggClientException;
import zingg.common.client.options.ZinggOptions;

import zingg.common.core.executor.FindAndLabeller;
import zingg.spark.core.context.ZinggSparkContext;


public class SparkFindAndLabeller extends FindAndLabeller<SparkSession, Dataset<Row>, Row, Column,DataType> {

	private static final long serialVersionUID = 1L;
	public static String name = "zingg.spark.core.executor.SparkFindAndLabeller";
	public static final Log LOG = LogFactory.getLog(SparkFindAndLabeller.class);

	public SparkFindAndLabeller() {
		setZinggOption(ZinggOptions.FIND_AND_LABEL);	
		ZinggSparkContext sparkContext = new ZinggSparkContext();
		setContext(sparkContext);
		finder = new SparkTrainingDataFinder(sparkContext);
		labeller = new SparkLabeller(sparkContext);
	}	
	
	@Override
	public void init(Arguments args) throws ZinggClientException {
		super.init(args);
		getContext().init();
	}	
	

}
