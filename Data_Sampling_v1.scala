import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{DataTypes, DoubleType, DecimalType}
import java.io.File
import java.io.PrintWriter
import scala.io.Source

//determine the class

val id_from  : String = "2018-09-02"
val id_to    : String = "2018-09-08"
val date_from: String = "2018-09-09"
val date_to  : String = "2018-09-15"
// Path to cds-data (rtb_data, lrtb, advangelist, etc) data on hdfs
//val urtb_data_path   =                "/sas/opt/etl/prod/events/d1_v5/year=2018/src=urtb/dt_log=2018-07-01"
//val lrtb_path        =                "/sas/opt/etl/prod/events/d1_v5/year=2018/src=lrtb/dt_log=2018-07-01"
//val ai_urtb_pca_path =  "maprfs://mapr5/sas/opt/etl/prod/pca/cds15/v1/year=201/src=ai_6010/dt_log=2018-07-01"
//val advangelist_data_path  =          "/sas/opt/etl/prod/pca/cds15/v1/year=2018/src=advangelist/dt_log=2018-09-10"
//val safegraph_path   =  "maprfs://mapr5/sas/opt/etl/prod/pca/cds15/v1/year=2018/src=safegraph/dt_log=2018-07-01"
val data_path = "maprfs://mapr5/sas/opt/etl/prod/pca/cds15/v1/year=2018/src=safegraph"
val sample_by_col = "did"
val frac : Double = 0.2
val seed = 124

//val store_path = "/sas/opt/etl/prod/events/d1_v5/year=2018/src=lrtb"

def store_cds15(df: DataFrame, ppath: String, num_part: Int = 50) = {
  //write cds15-dataframe to HDFS
  //df.write.paritionedBy("mycol1","mycol2").mode(SaveMode.Append).format("parquet").saveAsTable("myhivetable")

  val df_repartitioned = df.repartition(num_part)
  df_repartitioned.write.partitionBy("src", "dt").mode(SaveMode.Append).parquet(ppath)
  println("write to HDFS is done")
}

def sampleIdByTime (source_path: String, destination_path: String, id_from: String, id_to: String, frac: Double = 0.1, sample_by_col: String = "did", seed: Int = 42) : DataFrame = {

  //read data, filter by date-range and source, select ID-column, sample fraction of records from the column
  val data_source = spark.read.parquet(source_path)
  val IDs_sample_from_data = data_source.filter($"dt".between(id_from, id_to)).select(sample_by_col).distinct().sample(false,frac,seed)

  //determine date for writing the partition
  //val dt = java.time.LocalDate.now.toString
  val dt = id_to

  //when reading data from a file-path that includes partition-columns, those partitions get lost in the dataframe
  //recover source, i.e. last partition from the path: split path by '=' and take the last element
  val split_path = source_path.split("/")
  val source_partition = split_path(split_path.length - 1).split("=")(1)

  //add 'dt' and 'src' - columns to the ID-dataframe
  val IDs_sample_from_data_1 = IDs_sample_from_data.withColumn("src", lit(source_partition)).withColumn("dt", lit(dt))

  //write ID-sample data to a destination_path
  store_cds15(IDs_sample_from_data_1, destination_path)
}

def sampleIdByTimeFlex(data_path: String, sample_by_col: String, frac: Double, id_from: String, id_to: String, date_from: String, date_to: String, seed: Int = 42) : DataFrame = {
  //sampleIdByTime samples a fraction of IDs existing in time-range (id_from, id_to), and then selects all records associated with these IDs from date in time-range (date_from, date_to)
  //Input:
  //   data_path     - path to the parquet data,
  //   sample_by_col - column/feature in the dataframe to sample from,
  //   frac          - fraction of the data from the sample,
  //   id_from, id_to     - date-range from which IDs to be sampled,
  //   date_from, date_to - date-range from which Data to be sampled (for previously selected IDs),
  //   seed          - random seed for sampling,
  //Output: dataframe that contains records associated with sampled IDs

  val data_source = spark.read.parquet(data_path)
  val IDs_sample_from_data = data_source.filter($"dt".between(id_from, id_to)).select(sample_by_col).distinct().sample(false,frac,seed)

  val data_for_sampled_IDs = data_source.filter($"dt".between(date_from, date_to)).join(IDs_sample_from_data, Seq("did"))

  data_for_sampled_IDs
}

// Execution part
val sample_did_df = sampleIdByTime(data_path, sample_by_col, frac, id_from, id_to, date_from, date_to, seed)
println ("Sampled DF has Nrecords = " + sample_did_df.count())

val ppath = "/user/dzherebetskyy/tmp/hive/dzherebetskyy"
store_cds15(sample_did_df, ppath)