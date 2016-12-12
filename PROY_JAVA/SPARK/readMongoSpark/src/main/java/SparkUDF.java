import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

/**
 * Created by jsirpa on 08-12-16.
 */
public class SparkUDF {
    public SparkUDF(){}
    public SparkSession register(SparkSession spark){
        UDF1<Timestamp,Integer> m = (tmp) -> tmp.toLocalDateTime().getMonthValue();
        UDF1<Timestamp,Integer> d = (tmp) -> tmp.toLocalDateTime().getDayOfMonth();
        UDF1<Timestamp,Integer> y = (tmp) -> tmp.toLocalDateTime().getYear();
        UDF1<Timestamp,Integer> dw = (tmp) -> tmp.toLocalDateTime().getDayOfWeek().getValue();

        spark.udf().register("dm",d, DataTypes.IntegerType);
        spark.udf().register("m",m, DataTypes.IntegerType);
        spark.udf().register("y",y, DataTypes.IntegerType);
        spark.udf().register("dw",dw, DataTypes.IntegerType);
        return spark;
    }
}
