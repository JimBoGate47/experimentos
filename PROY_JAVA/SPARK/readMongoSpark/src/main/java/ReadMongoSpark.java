import breeze.optimize.linear.LinearProgram;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.Mongo;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka.KafkaUtils;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

import com.mongodb.spark.MongoSpark;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import org.bson.Document;
import scala.Tuple3;
import scala.Tuple7;
import kafka.serializer.StringDecoder;

/**
 * Created by jsirpa on 01-12-16.
 */

public class ReadMongoSpark {
    public static void main(String [] args){
    //    String uriMongo = "mongodb://localhost/streamSparkFinal.coll";
        String uriMongo = "mongodb://localhost/riot_main.coll";

        SparkConf sparkConf= new SparkConf()
                .setMaster("local[*]")
                .setAppName("loadMondoData")
                .set("spark.app.id","LoadDataMongoToSpark")
                .set("spark.mongodb.input.uri",uriMongo)
                .set("spark.mongodb.output.uri",uriMongo);
        JavaSparkContext jsc= new JavaSparkContext(sparkConf);
        SparkSession spark= SparkSession.builder().config(sparkConf).
                getOrCreate();

        Map<String, String> readOverrides = new HashMap<>();
     //   readOverrides.put("collection", "pruebaF");
        readOverrides.put("collection", "thingSnapshots");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        String[] timeRange= {
                "time",
                "2016-01-01 00:00:00.000",
                "2016-04-27 23:00:00.000"
        };
        String[][] parametros={
                {"value.serialNumber","serial"},
                {"value.zone.value.name","zone"},
                {"value.itemcode.value","itemcode"},
                {"value.sGroup.value","status"},
                {"value.status.value","group"}
        };

        String[] fields= Arrays.stream(parametros).map(x -> x[0]).toArray(String[]::new);
        String[] alias= Arrays.stream(parametros).map(x -> x[1]).toArray(String[]::new);


        Dataset<Row> df = MongoSpark.load(jsc,readConfig).toDF();

        Column[] columns1={col("Y"),col("M"),col("DM"),col("DW")};
        Column[] columns2=new Column[fields.length];

        String condition = fields[0] + " IS NOT NULL" ;
        String campos =  fields[0] +" AS " + alias[0] ;
        columns2[0]=col(alias[0]);
        for(int i=1;i<fields.length;i++) {
            condition = condition + " AND " + fields[i] + " IS NOT NULL";
            campos = campos + ", " + fields[i] + " AS " + alias[i];
            columns2[i] = col(alias[i]);
        }

        Column[]  columns= (Column[])ArrayUtils.addAll(columns1,columns2);


        //SQL queries
        df.createOrReplaceTempView("sharaf");
        Dataset<Row> df2= spark.sql(
                "SELECT " + timeRange[0] + ", " +campos +
                "\n FROM sharaf WHERE (" + timeRange[0] +
                " BETWEEN '"+timeRange[1]+ "' AND '" + timeRange[2] +"') AND "+ condition);
        //System.out.println(df2.take(1).toString());

        df2.createOrReplaceTempView("temporal");


        SparkUDAF ud= new SparkUDAF();
        spark.udf().register("ud",ud);


        Dataset<Row> df3 = spark.sql(
                "SELECT zone, ud(serial, itemCode, status, group) AS mapa FROM temporal GROUP BY zone"
        );
        df3.show(3);

        // Creating UDF to parse time and others.


        /*UDF1<Timestamp,Integer> m = (tmp) -> tmp.toLocalDateTime().getMonthValue();
        UDF1<Timestamp,Integer> d = (tmp) -> tmp.toLocalDateTime().getDayOfMonth();
        UDF1<Timestamp,Integer> y = (tmp) -> tmp.toLocalDateTime().getYear();
        UDF1<Timestamp,Integer> dw = (tmp) -> tmp.toLocalDateTime().getDayOfWeek().getValue();

        spark.udf().register("dm",d, DataTypes.IntegerType);
        spark.udf().register("m",m, DataTypes.IntegerType);
        spark.udf().register("y",y, DataTypes.IntegerType);
        spark.udf().register("dw",dw, DataTypes.IntegerType);

//        new SparkUDF().register(spark);

        Dataset<Row> df3=df2
                .withColumn("Y",callUDF("y",col(timeRange[0])))
                .withColumn("M",callUDF("m",col(timeRange[0])))
                .withColumn("DM",callUDF("dm",col(timeRange[0])))
                .withColumn("DW",callUDF("dw",col(timeRange[0])))
                .select(columns);*/


        //StructType inputschem = df2.select(columns2).schema();

        //JavaRDD<Row> rdd=df2.toJavaRDD();
        //df2.toJavaRDD().coalesce(1).saveAsTextFile("/home/jsirpa/borrarTest");
        //System.out.println(rdd.count());


        // To save de dateset in parquet format
        //df3.coalesce(1).write().parquet("prueba.parquet");

        //To read a parquet
        //Dataset<Row> df2 = spark.read().parquet("prueba.parquet");
        //df2.show(3);*/

    }

    //private
}
