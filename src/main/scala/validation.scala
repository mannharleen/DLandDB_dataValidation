import scala.util.{Try, Success, Failure}
import java.util.Properties
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import com.typesafe.config._
import scala.collection.JavaConverters._

// spark-shell --jars ojdbc6.jar,dlanddb_datavalidation_2.11-0.1.jar
//  -Dspark.master=local[*], -Dconfig.file=c:\\application.conf
/*
  Issues/Enhancements
  I-  Closed- when --tables= is not passed in params, it throws an error

 */
/*  The following inputs come from the application.conf file OR from the parameters -Dxx=yy:
      env
      username
      password
      driver
      ip
      port
      schemas
      tables
      table_keys
      dlpath
      dlaccesskey
      dlsecretkey

 */
object validation {
  def help:Unit = {
    println("\t\t**** Help ****<>")
    println(" Usage:             validation.start(\"--env=prod[/dev] --schemas=schema1[,schema2,..] --tables=table1[,table2]\") ")
    println(" example:           validation.start(\"--env=prod --schemas=FSCPHKL,FSCGPG --tables=AGEING_CBS,BBPATIENTBED\") ")
    //println(s"\n Schemas must be one of ${arr_schemas.mkString(",")} ")
    //println(s"\n Tables must be one of ${arr_tables.mkString(",")} ")
    println("\t\t**** created by harleen.mann****")
  }
  def main(args: Array[String]): Unit = {
    //println(ConfigFactory.load().getString("dev.db.ip"))
    //
    //call the mail_logic function for the application
    main_logic(args)
  }
  def start(input:String): Unit = {
    main_logic(Array(input))
  }
  def main_logic(args: Array[String]): Unit = {
    //Array("FSCPHKL", "FSCGKL", "FSCGPG", "FSCPHAK", "FSCPHI", "FSCPHP", "FSCPHC", "FSCPHK", "FSCPHBP", "FSCPHA", "FSCPHSP", "FSCPHM", "FSCGKK", "FSCGMH")
    //Array("AGEING_CBS", "AGEING_POST", "AGEING_POST_NEW", "AP_TEMP_BILLACCPERIOD", "AP_TEMP_BILLNO", "AP_TEMP_BOGUARANTORLETTER", "BBPATIENTBED", "BBPATIENTBEDHIST", "BOADJUSTMENT", "BOCAREPROVIDERFEETXN", "BOCHARGE", "BOCOLLECTION", "BODISCOUNT", "BOEPISODEACCOUNT", "BOEPISODEACCOUNTTXN", "BOEPISODEDEBTOR", "BOEPISODEDEBTORTXN", "BORECEIPT", "BOWRITEOFFTXN", "CBCUSTOMER", "CBDISCOUNT", "CBMISPAYMENT", "CBRECEIPT", "CBRECEIPTLINE", "CBREFUND", "CBREFUNDLINE", "CBRETURN", "CBSALE", "CBSALELINE", "CBTXN", "FDADDRESS", "FDDIAGNOSIS", "FDDIAGNOSISLINE", "FDEPISODE", "FDPATIENT", "FDPATIENTCATEGORYHIST", "FDPERSON", "GL_LASTPOSTDATE", "GL_OTHERPAYMENT", "GL_PA_TRANS_TYPE", "GL_POST", "GL_POST_FULL", "GLCCMATCHPOST", "GLCCMATCHPOSTLINE", "GLCCMATCHPOSTSUN", "GLCREDITCARD", "GLDAILYLOG", "GNACCOMMODATION", "GNACCOMMODATIONCHG", "GNBANK", "GNBED", "GNBILLINGGRP", "GNCAPPROFEE", "GNCAREPROVIDER", "GNCAREPROVIDERFEE", "GNCAREPROVIDERFEECLASS", "GNCAREPROVIDERFEELINE", "GNCATEGORY", "GNCHARGEITEM", "GNCHARGEITEMLINE", "GNCODE", "GNCODELINE", "GNCONTROLPARAMETER", "GNDEBTORTYPE_GLMAP", "GNDISCOUNT", "GNGLACCOUNT", "GNGLREVENUE", "GNICDCODE", "GNMERGEPROFEE", "GNORDERITEM", "GNORGANISATION", "GNPARAMETER", "GNPATIENTCLASS", "GNPAYMENTMODE_GLMAP", "GNPAYMENTMODETYPE", "GNPRORATEDPROFEE", "GNREVENUE", "GNROOM", "GNSIACCOUNT", "GNSISTOCK", "GNSISTOREROOM", "GNSITXNTYPE", "GNSUBCATEGORY", "GNTAX", "GNTAXLINE", "GNTXNCODE", "GNTXNTYPE", "GNUSER", "GNWARD", "JOURNALENTRY", "JOURNALENTRYLINE", "ODORDER", "ODORDERLINE", "ODORDERLINEDETAIL", "PHDRUGORDER", "PHDRUGORDERLINE", "PHDRUGPREPAREDISPENSE", "PHDRUGRETURN", "SIMOVEMENT", "SIMOVEMENTLINE", "SIOPENINGBALANCE", "SIOPENINGBALANCE_P", "SIPURCHASE", "SIPURCHASELINE", "SIRECEIPT", "SIRECEIPTLINE", "TEMP_AGEING_POST_RECEIVED", "TEMP_AGEING_POST_TXN", "VW_BEA_NO_WRO")
    //Map("AGEING_CBS" -> "JOURNALENTRY_ID", "AGEING_POST" -> "ACC_PERIOD", "AGEING_POST_NEW" -> "ACC_PERIOD", "AP_TEMP_BILLACCPERIOD" -> "MIN_ACC_PERIOD", "AP_TEMP_BILLNO" -> "JOURNALENTRYLINE_ID", "AP_TEMP_BOGUARANTORLETTER" -> "JOURNALENTRYLINE_ID", "CBMISPAYMENT" -> "CBMISPAYMENT_ID", "GL_LASTPOSTDATE" -> "ACC_PERIOD", "GL_OTHERPAYMENT" -> "GL_OTHERPAYMENT_ID", "GL_PA_TRANS_TYPE" -> "TRANS_REF", "GL_POST" -> "ACCOUNT_CODE", "GL_POST_FULL" -> "ACCOUNT_CODE", "GLCCMATCHPOST" -> "GLCCMATCHPOST_ID", "GLCCMATCHPOSTSUN" -> "ACCOUNT_CODE", "GNDEBTORTYPE_GLMAP" -> "GNDEBTORTYPE_GLMAP_ID", "GNPARAMETER" -> "PARAMETER_CODE", "GNPAYMENTMODE_GLMAP" -> "GNPAYMENTMODE_GLMAP_ID", "JOURNALENTRYLINE" -> "JOURNALENTRYLINE_ID", "SIOPENINGBALANCE" -> "GNSISTOCK_ID", "SIOPENINGBALANCE_P" -> "GNSISTOCK_ID", "SIRECEIPT" -> "SIRECEIPT_ID", "TEMP_AGEING_POST_RECEIVED" -> "ACC_PERIOD", "TEMP_AGEING_POST_TXN" -> "ACC_PERIOD", "VW_BEA_NO_WRO" -> "BOEPISODEACCOUNT_ID", "BBPATIENTBED" -> "BBPATIENTBED_ID", "BBPATIENTBEDHIST" -> "BBPATIENTBEDHIST_ID", "BOADJUSTMENT" -> "BOADJUSTMENT_ID", "BOCAREPROVIDERFEETXN" -> "BOCAREPROVIDERFEETXN_ID", "BOCHARGE" -> "BOCHARGE_ID", "BOCOLLECTION" -> "BOCOLLECTION_ID", "BODISCOUNT" -> "BODISCOUNT_ID", "BOEPISODEACCOUNT" -> "BOEPISODEACCOUNT_ID", "BOEPISODEACCOUNTTXN" -> "BOEPISODEACCOUNTTXN_ID", "BOEPISODEDEBTOR" -> "BOEPISODEDEBTOR_ID", "BOEPISODEDEBTORTXN" -> "BOEPISODEDEBTORTXN_ID", "BORECEIPT" -> "BORECEIPT_ID", "BOWRITEOFFTXN" -> "BOWRITEOFFTXN_ID", "CBCUSTOMER" -> "CBCUSTOMER_ID", "CBDISCOUNT" -> "CBDISCOUNT_ID", "CBRECEIPT" -> "CBRECEIPT_ID", "CBRECEIPTLINE" -> "CBRECEIPTLINE_ID", "CBREFUND" -> "CBREFUND_ID", "CBREFUNDLINE" -> "CBREFUNDLINE_ID", "CBRETURN" -> "CBRETURN_ID", "CBSALE" -> "CBSALE_ID", "CBSALELINE" -> "CBSALELINE_ID", "CBTXN" -> "CBTXN_ID", "FDADDRESS" -> "FDADDRESS_ID", "FDDIAGNOSIS" -> "FDDIAGNOSIS_ID", "FDDIAGNOSISLINE" -> "FDDIAGNOSISLINE_ID", "FDEPISODE" -> "FDEPISODE_ID", "FDPATIENT" -> "FDPATIENT_ID", "FDPATIENTCATEGORYHIST" -> "FDPATIENTCATEGORYHIST_ID", "FDPERSON" -> "FDPERSON_ID", "GLCCMATCHPOSTLINE" -> "GLCCMATCHPOSTLINE_ID", "GLCREDITCARD" -> "GLCREDITCARD_ID", "GLDAILYLOG" -> "GLDAILYLOG_ID", "GNACCOMMODATION" -> "GNACCOMMODATION_ID", "GNACCOMMODATIONCHG" -> "GNACCOMMODATIONCHG_ID", "GNBANK" -> "GNBANK_ID", "GNBED" -> "GNBED_ID", "GNBILLINGGRP" -> "GNBILLINGGRP_ID", "GNCAPPROFEE" -> "GNCAPPROFEE_ID", "GNCAREPROVIDER" -> "GNCAREPROVIDER_ID", "GNCAREPROVIDERFEE" -> "GNCAREPROVIDERFEE_ID", "GNCAREPROVIDERFEECLASS" -> "GNCAREPROVIDERFEECLASS_ID", "GNCAREPROVIDERFEELINE" -> "GNCAREPROVIDERFEELINE_ID", "GNCATEGORY" -> "GNCATEGORY_ID", "GNCHARGEITEM" -> "GNCHARGEITEM_ID", "GNCHARGEITEMLINE" -> "GNCHARGEITEMLINE_ID", "GNCODE" -> "GNCODE_ID", "GNCODELINE" -> "GNCODELINE_ID", "GNCONTROLPARAMETER" -> "HOSPITAL_CODE", "GNDISCOUNT" -> "GNDISCOUNT_ID", "GNGLACCOUNT" -> "GNGLACCOUNT_ID", "GNGLREVENUE" -> "GNGLREVENUE_ID", "GNICDCODE" -> "ICD_CODE", "GNMERGEPROFEE" -> "GNMERGEPROFEE_ID", "GNORDERITEM" -> "GNORDERITEM_ID", "GNORGANISATION" -> "GNORGANISATION_ID", "GNPATIENTCLASS" -> "GNPATIENTCLASS_ID", "GNPAYMENTMODETYPE" -> "PAYMENT_MODE", "GNPRORATEDPROFEE" -> "GNPRORATEDPROFEE_ID", "GNREVENUE" -> "GNREVENUE_ID", "GNROOM" -> "GNROOM_ID", "GNSIACCOUNT" -> "GNSIACCOUNT_ID", "GNSISTOCK" -> "GNSISTOCK_ID", "GNSISTOREROOM" -> "GNSISTOREROOM_ID", "GNSITXNTYPE" -> "GNSITXNTYPE_ID", "GNSUBCATEGORY" -> "GNSUBCATEGORY_ID", "GNTAX" -> "GNTAX_ID", "GNTAXLINE" -> "GNTAXLINE_ID", "GNTXNCODE" -> "GNTXNCODE_ID", "GNTXNTYPE" -> "GNTXNTYPE_ID", "GNUSER" -> "GNUSER_ID", "GNWARD" -> "GNWARD_ID", "JOURNALENTRY" -> "JOURNALENTRY_ID", "ODORDER" -> "ODORDER_ID", "ODORDERLINE" -> "ODORDERLINE_ID", "ODORDERLINEDETAIL" -> "ODORDERLINEDETAIL_ID", "PHDRUGORDER" -> "PHDRUGORDER_ID", "PHDRUGORDERLINE" -> "PHDRUGORDERLINE_ID", "PHDRUGPREPAREDISPENSE" -> "PHDRUGPREPAREDISPENSE_ID", "PHDRUGRETURN" -> "PHDRUGRETURN_ID", "SIMOVEMENT" -> "SIMOVEMENT_ID", "SIMOVEMENTLINE" -> "SIMOVEMENTLINE_ID", "SIPURCHASE" -> "SIPURCHASE_ID", "SIPURCHASELINE" -> "SIPURCHASELINE_ID", "SIRECEIPTLINE" -> "SIRECEIPTLINE_ID")
    //
    val args_map:Map[String, Array[String]]  = if (args.mkString.contains("--") && args.mkString.contains("=")) {
      args.mkString(" ").split(" --").map(x=> x.replace("-","")).map(x=> (x.split("=")(0), x.split("=")(1).split(","))).toMap
    } else Map(""->Array(""))
    //
    //
    val conf = ConfigFactory.load()
    val prop = new Properties
    val env: String = if (args_map.keys.exists(x=> x == "env")) args_map("env").head.toString else "prod" //default env is set to be prod
    val (db_ip:String, db_port:String, schemas:Array[String], tables:Array[String], table_keys: Map[String, String], s3path:String, s3access_key: String, s3secret_key: String) = {
      if (args_map.keys.exists(x=> x == "username")) {
        if (args_map("username").isEmpty) {
          prop.setProperty("user", conf.getString(s"$env.db.username"))
        } else prop.setProperty("user", args_map("username").head)
      } else prop.setProperty("user", conf.getString(s"$env.db.username"))
      if (args_map.keys.exists(x=> x == "password")) {
        if (args_map("password").isEmpty) {
          prop.setProperty("password", conf.getString(s"$env.db.password"))
        } else prop.setProperty("password", args_map("password").head)
      } else prop.setProperty("password", conf.getString(s"$env.db.password"))
      if (args_map.keys.exists(x=> x == "driver")) {
        if (args_map("driver").isEmpty) {
          prop.setProperty("driver", conf.getString(s"$env.db.driver"))
        } else prop.setProperty("driver", args_map("driver").head)
      } else prop.setProperty("driver", conf.getString(s"$env.db.driver"))
      val ip: String =
        if (args_map.keys.exists(x=> x == "ip")) {
          if (args_map("ip").isEmpty) {
            conf.getString(s"$env.db.ip")
          } else args_map("ip").head
        } else conf.getString(s"$env.db.ip")
      val port: String =
        if (args_map.keys.exists(x=> x == "port")) {
          if (args_map("port").isEmpty) {
            conf.getString(s"$env.db.port")
          } else args_map("port").head
        } else conf.getString(s"$env.db.port")
      val schemas: Array[String] =
        if (args_map.keys.exists(x=> x == "schemas")) {
          if (args_map("schemas").isEmpty) {
            conf.getStringList(s"$env.db.schemas").asScala.toArray
          } else args_map("schemas")
        } else conf.getStringList(s"$env.db.schemas").asScala.toArray
      val tables: Array[String] =
        if (args_map.keys.exists(x=> x == "tables")) {
          if (args_map("tables").isEmpty) {
            conf.getStringList(s"$env.db.tables").asScala.toArray
          } else args_map("tables")
        } else conf.getStringList(s"$env.db.tables").asScala.toArray
      //IMPO: table keys must be defined in the application.conf file
      val table_keys: Map[String, String] = (for {
        items <- conf.getObjectList(s"$env.db.tables_keys").asScala
        item <- items.entrySet().asScala
      } yield(item.getKey, item.getValue.unwrapped().toString) ).toMap
      val s3path: String =
        if (args_map.keys.exists(x=> x == "dlpath")) {
          if (args_map("dlpath").isEmpty) {
            conf.getString(s"$env.dl.path")
          } else args_map("dlpath").head
        } else conf.getString(s"$env.dl.path")
      val s3access_key:String =
        if (args_map.keys.exists(x=> x == "dlaccesskey")) {
          if (args_map("dlaccesskey").isEmpty) {
            conf.getString(s"$env.dl.access.key")
          } else args_map("dlaccesskey").head
        } else conf.getString(s"$env.dl.access.key")
      val s3secret_key:String =
        if (args_map.keys.exists(x=> x == "dlsecretkey")) {
          if (args_map("dlsecretkey").isEmpty) {
            conf.getString(s"$env.dl.secret.key")
          } else args_map("dlsecretkey").head
        } else conf.getString(s"$env.dl.secret.key")
      //
      (ip, port, schemas, tables, table_keys, s3path, s3access_key, s3secret_key)  //default points to production
    }
    val log_dir: String = conf.getString("log.dir")
    val out_file_handler = scala.tools.nsc.io.File(s"$log_dir/log_dl_db_val_out_${java.util.Calendar.getInstance().getTimeInMillis}.csv").createFile()
    // !!!E-use db_port by passing it to f_f
    // !!! E- help message currently not showing all options
    //
    //println(db_ip, db_port, schemas, tables, table_keys, s3path, prop.getProperty("user"))
    println("*** starting job <> <> ***")
    //

    schemas.map(schema => {
        var table_counter: Int = 0
        tables.map(table => {
            //println(s"starting for $table ")
            table_counter += 1
            Await.result(f_f(db_ip, db_port, schema, table, table_keys, prop, s3path, s3access_key, s3secret_key, table_counter, log_dir, out_file_handler), 24 hours)
        })
    })
  }
  def f_f(db_ip:String, db_port: String, schema: String, table: String, table_keys: Map[String, String], prop:Properties, s3path:String, s3access_key: String, s3secret_key:String, table_counter:Int, log_dir: String, out_file_handler: scala.tools.nsc.io.File) : Future[Int] = Future  {
    val spark = SparkSession.builder().appName("DLandDBvalidation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //this code works with both s3n and s3a (s3a is the recommended approach but it is not supported on AWs EMR)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3secret_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", s3access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", s3secret_key)

    println(s"Started - $schema.$table")
    var trying = Try()
    var df_cdc = spark.emptyDataFrame
    var df_base = spark.emptyDataFrame
    var df_base_cnt:Long = 0
    var df_cdc_i = spark.emptyDataFrame
    var df_cdc_d = spark.emptyDataFrame
    var df_cdc_u = spark.emptyDataFrame
    var df_cdc_i_cnt:Long = 0
    var df_cdc_d_cnt:Long = 0
    var df_dl_cnt:Long = 0
    var df_db_cnt:Long = 0
    var df_db_cnt_dist_key:Long = 0
    var df_db_sum_key:BigDecimal = 0.0
    //var df_db = spark.emptyDataFrame
    var df_dl_cnt_dist_key:Long = 0
    var df_dl_sum_key:BigDecimal = 0.0
    //
    var str_result_cnt:String = ""
    var str_result_cnt_dist_key:String = ""
    var str_result_sum_key:String = ""
    var df_db_schema: StructType = StructType(StructField(" ",StringType,true) :: Nil)
    //println("!!! inside f_f-2 " )
    val table_key:String = table_keys(table)
    //println("!!! inside f_f-1 " + s"(select count(distinct($table_key)) from $schema.$table)")
    try {
      spark.sparkContext.setJobDescription(s"DB data - $schema.$table")
      df_db_schema = spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select * from $schema.$table)",prop).schema
      val df_db_values: Array[Row] = if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
        spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select count(*) as cnt_,count(distinct($table_key)) as cd_, sum($table_key) as s_ from $schema.$table)",prop).collect
      } else spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select count(*) as cnt_,0.0 as cd_, 0.0 as s_ from $schema.$table)",prop).collect
      /*df_db_cnt = spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select count(*) from $schema.$table)",prop).first.mkString.toDouble.toInt
      df_db_cnt_dist_key = spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select count(distinct($table_key)) as cd_ from $schema.$table)",prop).first.mkString.toDouble.toInt
      df_db_sum_key = BigDecimal(spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select sum($table_key) as s_ from $schema.$table)",prop).first.mkString)
      */
      //println("%%%!!! "+df_db_values)
      df_db_cnt = df_db_values.head(0).asInstanceOf[java.math.BigDecimal].longValue()
      df_db_cnt_dist_key = df_db_values.head(1).asInstanceOf[java.math.BigDecimal].longValue()
      df_db_sum_key = df_db_values.head(2).asInstanceOf[java.math.BigDecimal]
    }
    catch {
      case e => e.printStackTrace
    }
    //println("!!! inside f_f0 " )
    spark.sparkContext.setJobDescription(s"DL Base data - $schema.$table")
    trying = Try(spark.read.option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/LOAD*"))
    df_base_cnt = 0
    df_dl_cnt_dist_key = 0
    //**** DF BASE
    trying match {
      case Success(v) => {
        df_base = spark.read.schema(df_db_schema).option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/LOAD*")
        if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
          //df_dl_cnt_dist_key = df_base.select(countDistinct(col(table_keys(table)))).first.mkString.toDouble.toInt
          //df_dl_sum_key = BigDecimal(df_base.select(sum(col(table_keys(table)))).first.mkString)
          val df_df_values_row = df_base.select(count(table_keys(table)), countDistinct(col(table_keys(table))), sum(col(table_keys(table))) ).first()
          df_base_cnt = df_df_values_row.getLong(0)
          df_dl_cnt_dist_key = df_df_values_row.getLong(1)
          df_dl_sum_key = df_df_values_row.getDecimal(2)
        } else {
          df_base_cnt = df_base.count
        }

      }
      case Failure(e) => println("Warning from the exception: " + e.getMessage)
    }
    //println("!!! inside f_f1")
    //**** DF CDC
    spark.sparkContext.setJobDescription(s"DL Base & CDC data - $schema.$table")
    trying = Try(spark.read.option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/201*"))
    //df_cdc = spark.emptyDataFrame
    df_cdc_i_cnt = 0
    df_cdc_d_cnt = 0
    trying match {
      case Success(v) => {
        df_cdc = spark.read.schema(StructType(StructField("I_U_D", StringType, true) +: df_db_schema.fields)).option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/201*").cache
        df_cdc_i = df_cdc.filter(col("I_U_D") === "I")
        df_cdc_d = df_cdc.filter(col("I_U_D") === "D")
        df_cdc_u = df_cdc.filter(col("I_U_D") === "U")
        df_cdc_i_cnt = df_cdc_i.count
        df_cdc_d_cnt = df_cdc_d.count
        if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
          //BigDecimal(df_base1.unionAll(df_cdc_i1).join(df_cdc_u1, Seq("BBPATIENTBEDHIST_ID"), "leftanti").unionAll(df_cdc_u1.dropDuplicates).select(sum(col("BBPATIENTBEDHIST_ID"))).na.fill(0).first.mkString) - BigDecimal(df_cdc.filter(col("I_U_D") === "D").select(sum(col("BBPATIENTBEDHIST_ID") )).na.fill(0).first.mkString)
          df_dl_cnt_dist_key = df_base.select(col(table_keys(table))).unionAll(df_cdc_i.select(col(table_keys(table)) )).except(df_cdc_d.select(col(table_keys(table)) ) ).select(countDistinct(col(table_keys(table)))).first.mkString.toDouble.toInt
          df_dl_sum_key = BigDecimal(df_base.select(col(table_keys(table))).unionAll(df_cdc_i.select(col(table_keys(table)))).join(df_cdc_u.select(col(table_keys(table))), Seq(table_keys(table)), "leftanti").unionAll(df_cdc_u.select(col(table_keys(table))).dropDuplicates).select(sum(col(table_keys(table)))).na.fill(0).first.mkString) - BigDecimal(df_cdc_d.select(sum(col(table_keys(table)))).na.fill(0).first.mkString)
        }
        //if ever required to have logic to give row num and then get latest update (required when one record is updated multiple times) use the following:
        /*
        import org.apache.spark.sql.expressions.Window
        val get_file_timestamp = udf((x: String) => x.substring(x.indexOfSlice("/201")+1).replace("-",""))
        val df_cdc_u1_temp = df_cdc.filter(col("I_U_D") === "U").select(col("BOCAREPROVIDERFEETXN_ID"), split(get_file_timestamp(input_file_name), "\\.")(0).as("timestamp_"))
        val df_cdc_u1_temp1 = df_cdc_u1_temp.withColumn("row_number", row_number.over(Window.orderBy(col("timestamp_") )) ).groupBy(col("BOCAREPROVIDERFEETXN_ID")).agg(max(col("row_number")))
        >> then inner join df_cdc_u1_temp1 with df_cdc_u1
        */
        // logic to get latest rows by applying I, U and D on base (better than above one)
        /*
        import org.apache.spark.sql.expressions.Window
        val get_file_timestamp = udf((x: String) => x.substring(x.indexOfSlice("/201")+1).replace("-",""))
        val df_cdc_latest = df_cdc.filter(col("I_U_D") === "U").select(col("MIN_ACC_PERIOD"), split(get_file_timestamp(input_file_name), "\\.")(0).as("timestamp_"), lit("U").as("I_U_D")).unionAll(df_cdc.filter(col("I_U_D") === "I").select(col("MIN_ACC_PERIOD"), split(get_file_timestamp(input_file_name), "\\.")(0).as("timestamp_"), lit("I"))).unionAll(df_cdc.filter(col("I_U_D") === "D").select(col("MIN_ACC_PERIOD"), split(get_file_timestamp(input_file_name), "\\.")(0).as("timestamp_"), lit("D"))).withColumn("global_row_number", row_number.over(Window.orderBy(col("timestamp_") )) ).withColumn("row_number", row_number.over(Window.partitionBy(col("MIN_ACC_PERIOD"),col("timestamp_") ).orderBy(col("global_row_number")) )).
             >> get the max row_number OR I could use a desc in orderby and select row_num=1
        */
      }
      case Failure(e) => println("Warning from the exception : " + e.getMessage)
    }
    //println("!!! inside f_f2")
    spark.sparkContext.setJobDescription(s"Result calculation - $schema.$table")
    df_dl_cnt = df_base_cnt + df_cdc_i_cnt - df_cdc_d_cnt
    val df_dl_minus_db_cnt = df_dl_cnt - df_db_cnt
    df_cdc.unpersist()
    if (df_dl_minus_db_cnt == 0) {
      str_result_cnt = "- OK -"
    } else if (df_dl_minus_db_cnt >= -10 && df_dl_minus_db_cnt <= 10) {
      str_result_cnt = "- !! EXCEPTION !! - LOW ****EXCEPTION**** "
    } else if (df_dl_minus_db_cnt >= -30  && df_dl_minus_db_cnt <= 30) {
      str_result_cnt = "- !! EXCEPTION !! - MEDIUM ****EXCEPTION****"
    } else str_result_cnt = "- !! EXCEPTION !! - HIGH ****EXCEPTION****"
    //
    var df_dl_minus_db_cnt_dist_key = BigDecimal("0")
    if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
      df_dl_minus_db_cnt_dist_key = df_dl_cnt_dist_key - df_db_cnt_dist_key
      if (df_dl_minus_db_cnt_dist_key == 0) {
        str_result_cnt_dist_key = "- OK -"
      } else if (df_dl_minus_db_cnt_dist_key >= -10 && df_dl_minus_db_cnt_dist_key <= 10) {
        str_result_cnt_dist_key = "- !! EXCEPTION !! - LOW ****EXCEPTION****"
      } else if (df_dl_minus_db_cnt_dist_key >= -30 && df_dl_minus_db_cnt_dist_key <= 30) {
        str_result_cnt_dist_key = "- !! EXCEPTION !! - MEDIUM ****EXCEPTION****"
      } else str_result_cnt_dist_key = "- !! EXCEPTION !! - HIGH ****EXCEPTION****"
    }
    //
    var df_dl_minus_db_sum_key = BigDecimal("0")
    if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
      df_dl_minus_db_sum_key = df_dl_sum_key - df_db_sum_key
      if (df_dl_minus_db_sum_key == 0) {
        str_result_sum_key = "- OK -"
      } else if (df_dl_minus_db_sum_key != 0 && (str_result_cnt == "- !! EXCEPTION !! - LOW" || str_result_cnt_dist_key == "- !! EXCEPTION !! - LOW" ) ) {
        str_result_sum_key = "- !! EXCEPTION !! - LOW ****EXCEPTION****"
      } else  if (df_dl_minus_db_sum_key != 0 && (str_result_cnt == "- !! EXCEPTION !! - MEDIUM" || str_result_cnt_dist_key == "- !! EXCEPTION !! - MEDIUM") ) {
        str_result_sum_key = "- !! EXCEPTION !! - MEDIUM ****EXCEPTION****"
      } else str_result_sum_key = "- !! EXCEPTION !! - HIGH ****EXCEPTION****"
    }
    //
    var message1:String = s"**RowCount**,$schema,$table,," + "base+I-D=DL||DB," + df_base_cnt +",+" + df_cdc_i_cnt + ",-" + df_cdc_d_cnt + ",=" + df_dl_cnt + ",||," + df_db_cnt + ", \t\t >> ," + ",cnt_diff = " +df_dl_minus_db_cnt + str_result_cnt
    var message2:String = ""
    var message3:String = ""
    //println(s"**RowCount**,$schema,$table,," + "base+I-D=DL||DB," + df_base_cnt +",+" + df_cdc_i_cnt + ",-" + df_cdc_d_cnt + ",=" + df_dl_cnt + ",||," + df_db_cnt + ", \t\t >> ," + ",cnt_diff = " +df_dl_minus_db_cnt + str_result_cnt)
    if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
      //println(s"**DistinctKeyCount**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_cnt_dist_key + ",||," + df_db_cnt_dist_key + ", \t\t >> ," + ",dist_diff = " +df_dl_minus_db_cnt_dist_key + str_result_cnt_dist_key)
      //println(s"**sumKey**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_sum_key + ",||," + df_db_sum_key + ", \t\t >> ," + ",sum_diff = " +df_dl_minus_db_sum_key + str_result_sum_key)
      message2 = s"**DistinctKeyCount**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_cnt_dist_key + ",||," + df_db_cnt_dist_key + ", \t\t >> ," + ",dist_diff = " +df_dl_minus_db_cnt_dist_key + str_result_cnt_dist_key
      message3 = s"**sumKey**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_sum_key + ",||," + df_db_sum_key + ", \t\t >> ," + ",sum_diff = " +df_dl_minus_db_sum_key + str_result_sum_key
    } else {
      //println(s"**DistinctKeyCount**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_cnt_dist_key + ",||," + ", \t\t >> ," + "Warning: No PK detected" + df_db_cnt_dist_key )
      //println(s"**sumKey**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_sum_key + ",||," + ", \t\t >> ," + "Warning: No PK detected" + df_db_sum_key )
      message2 = s"**DistinctKeyCount**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_cnt_dist_key + ",||," + ", \t\t >> ," + "Warning: No PK detected" + df_db_cnt_dist_key
      message3 = s"**sumKey**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_sum_key + ",||," + ", \t\t >> ," + "Warning: No PK detected" + df_db_sum_key
    }
    val arr_print_results:Array[String] = Array(message1, message2, message3)
    out_file_handler.appendAll(arr_print_results.mkString("\n")+"\n")
    //println("!!! inside f_f3")
    if (table_counter == table_keys.size) println("**END****END****END****END**, "+println(java.util.Calendar.getInstance.getTime))
    1
  }
}