package zingg.spark.core.feature;

import java.util.HashMap;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import zingg.common.core.feature.ArrayDoubleFeature;
import zingg.common.core.feature.DateFeature;
import zingg.common.core.feature.DoubleFeature;
import zingg.common.core.feature.FeatureFactory;
import zingg.common.core.feature.FloatFeature;
import zingg.common.core.feature.IntFeature;
import zingg.common.core.feature.LongFeature;
import zingg.common.core.feature.StringFeature;

public class SparkFeatureFactory extends FeatureFactory<DataType>{

	public static final String ARR_DOUBLE_TYPE_STR = "\"ARR_DOUBLE_TYPE\"";

	private static final long serialVersionUID = 1L;
    
    @Override
    public void init() {
            map = new HashMap<DataType, Class>();
            map.put(DataTypes.StringType, StringFeature.class);
            map.put(DataTypes.IntegerType, IntFeature.class);
            map.put(DataTypes.DateType, DateFeature.class);
            map.put(DataTypes.DoubleType, DoubleFeature.class);
            map.put(DataTypes.FloatType, FloatFeature.class);
            map.put(DataTypes.LongType, LongFeature.class);
            map.put(DataTypes.createArrayType(DataTypes.DoubleType), ArrayDoubleFeature.class);
    }

    @Override
    public DataType getDataTypeFromString(String t) {
        if (ARR_DOUBLE_TYPE_STR.equals(t)) {
        	return DataTypes.createArrayType(DataTypes.DoubleType);
        } else {
        	return DataType.fromJson(t);
        }
    }
    
}
