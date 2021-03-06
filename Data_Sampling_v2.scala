import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{DataTypes, DoubleType, DecimalType}
import org.apache.spark.sql._
import java.io.File
import java.io.PrintWriter
import scala.io.Source

import org.apache.spark.sql.implicits._

val x = 1
// Path to cds-data (rtb_data, lrtb, advangelist, etc) data on hdfs
//val urtb_data_path   =                "/sas/opt/etl/prod/events/d1_v5/year=2018/src=urtb/dt_log=2018-07-01"
//val lrtb_path        =                "/sas/opt/etl/prod/events/d1_v5/year=2018/src=lrtb/dt_log=2018-07-01"
//val ai_urtb_pca_path =  "maprfs://mapr5/sas/opt/etl/prod/pca/cds15/v1/year=201/src=ai_6010/dt_log=2018-07-01"
//val advangelist_data_path  =          "/sas/opt/etl/prod/pca/cds15/v1/year=2018/src=advangelist/dt_log=2018-09-10"
//val safegraph_path   =  "maprfs://mapr5/sas/opt/etl/prod/pca/cds15/v1/year=2018/src=safegraph/dt_log=2018-07-01"
//val data_path = "/sas/opt/etl/prod/events/d1_v5/year=2018/"
//val id_dest_path = "/user/dzherebetskyy/tmp/hive/dzherebetsky"
//val sample_dest_path = "/user/dzherebetskyy/tmp/hive/dzherebetsky"


class DataSampling(source_path: String, src_partition: String, id_destination_path: String, sample_write_path: String) {
  var src_path: String = source_path
  var src_part: String = src_partition
  var dest_path_id: String = id_destination_path
  var path_downsample: String = sample_write_path

  def store_cds15(df: DataFrame, ppath: String) = {
    // write a dataframe <df> to the <ppath> on HDFS
    //write cds15-dataframe to HDFS
    //df.write.paritionedBy("mycol1","mycol2").mode(SaveMode.Append).format("parquet").saveAsTable("myhivetable")

    df.write.partitionBy("src", "dt").mode(SaveMode.Append).parquet(ppath)
    println("write to HDFS is done")
  }

  def sampleIdByTime (id_from: String, id_to: String, frac: Double, src_path: String, src: String, sample_by_col: String = "did", num_part: Int = 50, seed: Int = 42):DataFrame = {
    // Sample a fraction of IDs from a dataframe in some time-range
    //id_from, id_to - date range for sampling IDs
    //src_path - where from to read initial data that is used to sample IDs
    // dest_path - where to write the ID-dataframe,
    // frac - fraction of IDs that we want to sample,
    //sample_by_col - name of the ID-column that we want to sample from,
    // num_part - number of partitions that we want to have in the sampled ID-dataframe (by default it could be 512 or 1024, etc. and we do not need so many)
    // seed -  seed for the random sampling (for results reproducability)

    //read data, filter by date-range and source, select ID-column, sample fraction of records from the column
    val data = spark.read.parquet(src_path)
    //val IDs_sample_from_data = data_source.filter($"dt".between(id_from, id_to) && $"src" === src).select(sample_by_col, "src").distinct().sample(false,frac,seed)
    //val IDs_sample_from_data = data.filter(date_format(data("dt"), "yyyy-MM-dd").between("2018-09-05", "2018-09-08"))
    val IDs_sample_from_data = data.filter(data("dt").between(id_from, id_to)).filter("src = " + "'"+src+"'").select(sample_by_col, "src").distinct().sample(false,frac,seed)
    println("==============     INFO: IDs downsampling success, proceed to adding dt and repartition     ==============")

    //determine date for writing the partition
    //val dt = java.time.LocalDate.now.toString
    val dt = id_to

    //when reading data from a file-path that includes partition-columns, those partitions get lost in the dataframe
    //recover source, i.e. last partition from the path: split path by '=' and take the last element
    //val split_path = src_path.split("/")
    //val source_partition = split_path(split_path.length - 1).split("=")(1)
    //add 'dt' and 'src' - columns to the ID-dataframe
    //val IDs_sample_from_data_1 = IDs_sample_from_data.withColumn("src", lit(source_partition)).withColumn("dt", lit(dt))
    val IDs_sample_from_data_1 = IDs_sample_from_data.withColumn("dt", lit(dt))

    //write ID-sample data to a destination_path, with repartitioning to 50 files
    val IDs_sample_from_data_2 = IDs_sample_from_data_1.repartition(num_part)
    //println("number of partitions = "+ IDs_sample_from_data_2.rdd.partitions.size)
    //store_cds15(IDs_sample_from_data_2, dest_path)
    //IDs_sample_from_data_2.printSchema
    println("==============     INFO: Repartitioning success, proceed to next function     ==============")

    IDs_sample_from_data_2
  }

  def downsampleDataByID (id_date: String, day_for_downsampling: String, path_read_id: String, path_daily_data: String, src: String): DataFrame = {
    //returns DataFrame that has all daily records associated with IDs
    // path_read_id - path to downsampled IDs (from previous week)
    // id_sampling_date - when the weekly ID-sampling was done
    // path_daily_data - path to daily data from which we get records associated with the IDs
    // day_for_downsampling - date in daily data from which we want to get the records
    // src  - src-partition for the ID-data

    //val id_downsampled = spark.read.parquet(path_read_id).filter($"src" === src && $"dt" === id_date).select("did")
    val id_downsampled = spark.read.parquet(path_read_id).filter("src = " + "'"+src+ "'").filter("dt = " + "'"+id_date+ "'").select("did")
    println("==============     INFO: IDs read success, proceed to daily data            ==============")
    val new_incoming_data = spark.read.parquet(path_daily_data).filter("dt = " + "'"+day_for_downsampling+ "'")
    println("==============     INFO: Daily data read success, proceed to downsampling   ==============")
    val new_data_downsampled_by_IDs = new_incoming_data.join(id_downsampled, Seq("did"))
    println("==============     INFO: Downsampling is done, proceed to next procedure    ==============")

    new_data_downsampled_by_IDs
  }

  def run_get_IDs_weekly (id_from: String, id_to: String, frac: Double = 0.05) = {
    val df_ID = sampleIdByTime(id_from, id_to, frac, src_path, src_part)
    store_cds15(df_ID, dest_path_id)
  }

  def run_get_records_by_ID_daily (id_sampling_date: String, day_to_downsample: String) = {
    val df_downsampled_data_by_ID = downsampleDataByID(id_sampling_date, day_to_downsample, dest_path_id, src_path, src_part)
    store_cds15(df_downsampled_data_by_ID, path_downsample)
  }

}

def test()={
  //    -----------  Class Test -----------
  val data_path = "/sas/opt/etl/prod/events/d1_v5/year=2018/"
  val id_dest_path = "/user/dzherebetskyy/tmp/hive/dzherebetsky/id_table"
  val sample_dest_path = "/user/dzherebetskyy/tmp/hive/dzherebetsky/downsample"
  val source = "lrtb"

  val dsc = new DataSampling(data_path, source, id_dest_path, sample_dest_path)

  val id_from : String = "2018-09-09"
  val id_to   : String = "2018-09-15"
  val id_frac : Double = 0.05

  dsc.run_get_IDs_weekly(id_from, id_to, id_frac)

  val some_day : String = "2018-09-17"
  dsc.run_get_records_by_ID_daily(id_to, some_day)


  //    -----------  Functions Test -----------
  // val df1 = sampleIdByTime(id_from, id_to, data_path, source, frac)

  //val df_downsampled_data_by_ID = downsampleDataByID(id_dest_path, id_to, data_path, some_day, src)
  //df_downsampled_data_by_ID.count()
  // df_downsampled_data_by_ID.printSchema()
  //store_cds15(df_downsampled_data_by_ID, sdp)

  //val id_from  : String = "2018-09-02"
  //val id_to    : String = "2018-09-08"
  //val date_from: String = "2018-09-09"
  //val date_to  : String = "2018-09-15"
  //val some_day : String = "2018-09-11"

  //val sample_by_col = "did"
  //val frac : Double = 0.05
  //val seed = 124

}
