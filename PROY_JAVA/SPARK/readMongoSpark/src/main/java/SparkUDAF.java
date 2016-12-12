import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkUDAF extends UserDefinedAggregateFunction
{
    private StructType inputSchema;
    private StructType bufferSchema;
    private DataType returnDataType= DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
    MutableAggregationBuffer mutableBuffer;
    public SparkUDAF(){
        List<StructField> inputFields = new ArrayList<>();
        List<StructField> inputFields2;
//        inputFields.add(DataTypes.createStructField("time",DataTypes.TimestampType,true));
        inputFields.add(DataTypes.createStructField("zone",DataTypes.StringType,true));
        inputFields.add(DataTypes.createStructField("item",DataTypes.StringType,true));
        inputFields.add(DataTypes.createStructField("status",DataTypes.StringType,true));
        inputFields.add(DataTypes.createStructField("group",DataTypes.StringType,true));
        inputSchema=DataTypes.createStructType(inputFields);

        inputFields2=inputFields;
        inputFields2.add(DataTypes.createStructField("outputMap",DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
        bufferSchema=DataTypes.createStructType(inputFields2);

        //returnDataType=inputSchema;
    }



    /*private DataType returnDataType =
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);


    public SparkUDAF()
    {
//inputSchema : This UDAF can accept 2 inputs which are of type Integer
        List<StructField> inputFields = new ArrayList<>();
        StructField inputStructField1 = DataTypes.createStructField("femaleCount",DataTypes.IntegerType, true);
        inputFields.add(inputStructField1);
        StructField inputStructField2 = DataTypes.createStructField("maleCount",DataTypes.IntegerType, true);
        inputFields.add(inputStructField2);
        inputSchema = DataTypes.createStructType(inputFields);

//BufferSchema : This UDAF can hold calculated data in below mentioned buffers
        List<StructField> bufferFields = new ArrayList<>();
        StructField bufferStructField1 = DataTypes.createStructField("totalCount",DataTypes.IntegerType, true);
        bufferFields.add(bufferStructField1);
        StructField bufferStructField2 = DataTypes.createStructField("femaleCount",DataTypes.IntegerType, true);
        bufferFields.add(bufferStructField2);
        StructField bufferStructField3 = DataTypes.createStructField("maleCount",DataTypes.IntegerType, true);
        bufferFields.add(bufferStructField3);
        StructField bufferStructField4 = DataTypes.createStructField("outputMap",DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
        bufferFields.add(bufferStructField4);
        bufferSchema = DataTypes.createStructType(bufferFields);
    }*/

    /**
     * This method determines which bufferSchema will be used
     */
    @Override
    public StructType bufferSchema() {

        return bufferSchema;
    }

    /**
     * This method determines the return type of this UDAF
     */
    @Override
    public DataType dataType() {
        return returnDataType;
    }

    /**
     * Returns true iff this function is deterministic, i.e. given the same input, always return the same output.
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * This method will re-initialize the variables to 0 on change of city name
     */
    /*@Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0);
        buffer.update(1, 0);
        buffer.update(2, 0);
        mutableBuffer = buffer;
    }*/

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
        buffer.update(1, "");
        buffer.update(2, "");
        buffer.update(3, "");
       // buffer.update(4, 0);
        mutableBuffer = buffer;
    }

    /**
     * This method is used to increment the count for each city
     */
    /*@Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0, buffer.getInt(0) + input.getInt(0) + input.getInt(1));
        buffer.update(1, input.getInt(0));
        buffer.update(2, input.getInt(1));
    }*/

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0, input.getString(0));
        buffer.update(1, input.getString(1));
        buffer.update(2, input.getString(2));
        buffer.update(3, input.getString(3));
       // buffer.update(4, input.getInt(4));
    }


    /**
     * This method will be used to merge data of two buffers
     */

    /*
    @Override
    public void merge(MutableAggregationBuffer buffer, Row input) {

        buffer.update(0, buffer.getInt(0) + input.getInt(0));
        buffer.update(1, buffer.getInt(1) + input.getInt(1));
        buffer.update(2, buffer.getInt(2) + input.getInt(2));

    }*/

    @Override
    public void merge(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0, buffer.getString(0) + input.getString(0));
        buffer.update(1, buffer.getString(1) + input.getString(1));
        buffer.update(2, buffer.getString(2) + input.getString(2));
        buffer.update(3, buffer.getString(3) + input.getString(3));
      //  buffer.update(4, buffer.getInt(4) + input.getInt(4));

    }

    /**
     * This method calculates the final value by referring the aggregation buffer
     */
    /*@Override
    public Object evaluate(Row buffer) {
//In this method we are preparing a final map that will be returned as output
        Map<String,String> op = new HashMap<>();
        op.put("Total", "" + mutableBuffer.getInt(0));
        op.put("dominant", "Male");
        if(buffer.getInt(1) > mutableBuffer.getInt(2))
        {
            op.put("dominant", "Female");
        }
        mutableBuffer.update(3,op);

        return buffer.getMap(3);
    }*/

    @Override
    public Object evaluate(Row buffer) {
//In this method we are preparing a final map that will be returned as output
        Map<String,String> op = new HashMap<>();
        //op.put("Total", "" + mutableBuffer.getInt(0));
        op.put("dominant", "Male");
        op.put("Size", "" + mutableBuffer.getString(0));
        op.put("group", "" + mutableBuffer.getString(1));
        op.put("ser", "" + mutableBuffer.getString(2));
        mutableBuffer.update(4,op);

        return buffer.getMap(4);
    }

    /**
     * This method will determine the input schema of this UDAF
     */
    @Override
    public StructType inputSchema() {

        return inputSchema;
    }

}
