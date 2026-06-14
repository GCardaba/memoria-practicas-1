import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ConvertToParquet {
def main(args: Array[String]): Unit = {
  
  println("🍎 Conversión TPC-H → Parquet (Scala)")
  println("=" * 50)
  println()
  
  // Configurar Spark
  val spark = SparkSession.builder()
    .appName("TPC-H to Parquet")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .getOrCreate()
  
  spark.sparkContext.setLogLevel("WARN")
  
  // Definir schemas
  val regionSchema = StructType(Array(
    StructField("r_regionkey", IntegerType, nullable = false),
    StructField("r_name", StringType, nullable = false),
    StructField("r_comment", StringType, nullable = true),
    StructField("skip", StringType, nullable = true)
  ))
  
  val nationSchema = StructType(Array(
    StructField("n_nationkey", IntegerType, nullable = false),
    StructField("n_name", StringType, nullable = false),
    StructField("n_regionkey", IntegerType, nullable = false),
    StructField("n_comment", StringType, nullable = true),
    StructField("skip", StringType, nullable = true)
  ))
  
  val customerSchema = StructType(Array(
    StructField("c_custkey", IntegerType, nullable = false),
    StructField("c_name", StringType, nullable = false),
    StructField("c_address", StringType, nullable = false),
    StructField("c_nationkey", IntegerType, nullable = false),
    StructField("c_phone", StringType, nullable = false),
    StructField("c_acctbal", DecimalType(15,2), nullable = false),
    StructField("c_mktsegment", StringType, nullable = false),
    StructField("c_comment", StringType, nullable = false),
    StructField("skip", StringType, nullable = true)
  ))
  
  val supplierSchema = StructType(Array(
    StructField("s_suppkey", IntegerType, nullable = false),
    StructField("s_name", StringType, nullable = false),
    StructField("s_address", StringType, nullable = false),
    StructField("s_nationkey", IntegerType, nullable = false),
    StructField("s_phone", StringType, nullable = false),
    StructField("s_acctbal", DecimalType(15,2), nullable = false),
    StructField("s_comment", StringType, nullable = false),
    StructField("skip", StringType, nullable = true)
  ))
  
  val partSchema = StructType(Array(
    StructField("p_partkey", IntegerType, nullable = false),
    StructField("p_name", StringType, nullable = false),
    StructField("p_mfgr", StringType, nullable = false),
    StructField("p_brand", StringType, nullable = false),
    StructField("p_type", StringType, nullable = false),
    StructField("p_size", IntegerType, nullable = false),
    StructField("p_container", StringType, nullable = false),
    StructField("p_retailprice", DecimalType(15,2), nullable = false),
    StructField("p_comment", StringType, nullable = false),
    StructField("skip", StringType, nullable = true)
  ))
  
  val partsuppSchema = StructType(Array(
    StructField("ps_partkey", IntegerType, nullable = false),
    StructField("ps_suppkey", IntegerType, nullable = false),
    StructField("ps_availqty", IntegerType, nullable = false),
    StructField("ps_supplycost", DecimalType(15,2), nullable = false),
    StructField("ps_comment", StringType, nullable = false),
    StructField("skip", StringType, nullable = true)
  ))
  
  val ordersSchema = StructType(Array(
    StructField("o_orderkey", IntegerType, nullable = false),
    StructField("o_custkey", IntegerType, nullable = false),
    StructField("o_orderstatus", StringType, nullable = false),
    StructField("o_totalprice", DecimalType(15,2), nullable = false),
    StructField("o_orderdate", DateType, nullable = false),
    StructField("o_orderpriority", StringType, nullable = false),
    StructField("o_clerk", StringType, nullable = false),
    StructField("o_shippriority", IntegerType, nullable = false),
    StructField("o_comment", StringType, nullable = false),
    StructField("skip", StringType, nullable = true)
  ))
  
  val lineitemSchema = StructType(Array(
    StructField("l_orderkey", IntegerType, nullable = false),
    StructField("l_partkey", IntegerType, nullable = false),
    StructField("l_suppkey", IntegerType, nullable = false),
    StructField("l_linenumber", IntegerType, nullable = false),
    StructField("l_quantity", DecimalType(15,2), nullable = false),
    StructField("l_extendedprice", DecimalType(15,2), nullable = false),
    StructField("l_discount", DecimalType(15,2), nullable = false),
    StructField("l_tax", DecimalType(15,2), nullable = false),
    StructField("l_returnflag", StringType, nullable = false),
    StructField("l_linestatus", StringType, nullable = false),
    StructField("l_shipdate", DateType, nullable = false),
    StructField("l_commitdate", DateType, nullable = false),
    StructField("l_receiptdate", DateType, nullable = false),
    StructField("l_shipinstruct", StringType, nullable = false),
    StructField("l_shipmode", StringType, nullable = false),
    StructField("l_comment", StringType, nullable = false),
    StructField("skip", StringType, nullable = true)
  ))
  
  // Mapa de tablas y schemas
  val tables = Map(
    "region" -> regionSchema,
    "nation" -> nationSchema,
    "supplier" -> supplierSchema,
    "customer" -> customerSchema,
    "part" -> partSchema,
    "partsupp" -> partsuppSchema,
    "orders" -> ordersSchema,
    "lineitem" -> lineitemSchema
  )
  
  val outputDir = "parquet_data"
  
  // Procesar cada tabla
  tables.foreach { case (tableName, schema) =>
    print(s"📦 $tableName... ")
    
    val startTime = System.currentTimeMillis()
    
    // Leer archivo .tbl
    val df = spark.read
      .option("delimiter", "|")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .csv(s"$tableName.tbl")
      .drop("skip")
    
    // Cache para contar
    df.cache()
    val count = df.count()
    
    // Guardar como Parquet
    val outputPath = s"$outputDir/$tableName"
    
    tableName match {
      case "lineitem" => df.repartition(8).write.mode("overwrite").parquet(outputPath)
      case "orders" | "partsupp" => df.repartition(4).write.mode("overwrite").parquet(outputPath)
      case _ => df.write.mode("overwrite").parquet(outputPath)
    }
    
    df.unpersist()
    
    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
    
    println(f"✅ ${count}%,d filas (${elapsed}%.1fs)")
  }
  
  println()
  println("=" * 50)
  println("✅ Conversión completada!")
  println()
  println(s"📂 Datos Parquet en: ./$outputDir/")
  
  spark.stop()
}
}

// Llamar a main si se ejecuta como script
ConvertToParquet.main(Array())
