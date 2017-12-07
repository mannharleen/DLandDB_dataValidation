import scala.util.{Try, Success, Failure}
import java.util.Properties
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// spark-shell --jars ojdbc6.jar,dlanddb_datavalidation_2.11-0.1.jar

object validation {
  val spark = SparkSession.builder().appName("DLandDBvalidation").getOrCreate()
  def help = {
    println("**** Help ****")
    println(" Usage:             validation.main(\"--env=prod[/uat] --schemas=schema1[,schema2,..] --tables=table1[,table2]\") ")
    println(" example:           validation.main(\"--env=prod --schemas=FSCPHKL,FSCGPG --tables=AGEING_CBS,BBPATIENTBED\") ")
    println("**** created by harleen.mann@parkwaypantai.com ****")
  }
  def start(args:String): Unit = {
    main(Array(args))
  }
  def main(args: Array[String]): Unit = {
    // on scala> do something like:-
    val args_map:Map[String, Array[String]] = args.mkString(" ").split(" --").map(x=> x.replace("-","")).map(x=> (x.split("=")(0), x.split("=")(1).split(","))).toMap
    //
    spark.sparkContext.setLogLevel("ERROR")
    //
    val db_ip:String = if (args_map("env").mkString(",") == "uat") {
      "172.18.20.70"
    } else "172.18.20.140"  //default points to production
    //
    val s3path:String = if (args_map("env").mkString(",") == "uat") {
      "s3://datalake-uat/Fisicien/DMS-only"
    } else "s3://datalake-prod-env/Fisicien/DMS-only" //default points to production
    //
    val prop = new Properties
    prop.setProperty("user", "LOGMNR_USER")
    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    if (args_map("env").mkString(",") == "uat") {
      prop.setProperty("password", "pantai123")
    } else prop.setProperty("password", "welcome123") //default points to production
    //
    val schemas: Array[String] = if (args_map("schemas").isEmpty) {
      Array("FSCPHKL", "FSCGKL", "FSCGPG", "FSCPHAK", "FSCPHI", "FSCPHP", "FSCPHC", "FSCPHK", "FSCPHBP", "FSCPHA", "FSCPHSP", "FSCPHM", "FSCGKK", "FSCGMH")
    } else args_map("schemas")
    val tables: Array[String] = if (args_map("tables").isEmpty) {
      Array("AGEING_CBS", "AGEING_POST", "AGEING_POST_NEW", "AP_TEMP_BILLACCPERIOD", "AP_TEMP_BILLNO", "AP_TEMP_BOGUARANTORLETTER", "BBPATIENTBED", "BBPATIENTBEDHIST", "BOADJUSTMENT", "BOCAREPROVIDERFEETXN", "BOCHARGE", "BOCOLLECTION", "BODISCOUNT", "BOEPISODEACCOUNT", "BOEPISODEACCOUNTTXN", "BOEPISODEDEBTOR", "BOEPISODEDEBTORTXN", "BORECEIPT", "BOWRITEOFFTXN", "CBCUSTOMER", "CBDISCOUNT", "CBMISPAYMENT", "CBRECEIPT", "CBRECEIPTLINE", "CBREFUND", "CBREFUNDLINE", "CBRETURN", "CBSALE", "CBSALELINE", "CBTXN", "FDADDRESS", "FDDIAGNOSIS", "FDDIAGNOSISLINE", "FDEPISODE", "FDPATIENT", "FDPATIENTCATEGORYHIST", "FDPERSON", "GL_LASTPOSTDATE", "GL_OTHERPAYMENT", "GL_PA_TRANS_TYPE", "GL_POST", "GL_POST_FULL", "GLCCMATCHPOST", "GLCCMATCHPOSTLINE", "GLCCMATCHPOSTSUN", "GLCREDITCARD", "GLDAILYLOG", "GNACCOMMODATION", "GNACCOMMODATIONCHG", "GNBANK", "GNBED", "GNBILLINGGRP", "GNCAPPROFEE", "GNCAREPROVIDER", "GNCAREPROVIDERFEE", "GNCAREPROVIDERFEECLASS", "GNCAREPROVIDERFEELINE", "GNCATEGORY", "GNCHARGEITEM", "GNCHARGEITEMLINE", "GNCODE", "GNCODELINE", "GNCONTROLPARAMETER", "GNDEBTORTYPE_GLMAP", "GNDISCOUNT", "GNGLACCOUNT", "GNGLREVENUE", "GNICDCODE", "GNMERGEPROFEE", "GNORDERITEM", "GNORGANISATION", "GNPARAMETER", "GNPATIENTCLASS", "GNPAYMENTMODE_GLMAP", "GNPAYMENTMODETYPE", "GNPRORATEDPROFEE", "GNREVENUE", "GNROOM", "GNSIACCOUNT", "GNSISTOCK", "GNSISTOREROOM", "GNSITXNTYPE", "GNSUBCATEGORY", "GNTAX", "GNTAXLINE", "GNTXNCODE", "GNTXNTYPE", "GNUSER", "GNWARD", "JOURNALENTRY", "JOURNALENTRYLINE", "ODORDER", "ODORDERLINE", "ODORDERLINEDETAIL", "PHDRUGORDER", "PHDRUGORDERLINE", "PHDRUGPREPAREDISPENSE", "PHDRUGRETURN", "SIMOVEMENT", "SIMOVEMENTLINE", "SIOPENINGBALANCE", "SIOPENINGBALANCE_P", "SIPURCHASE", "SIPURCHASELINE", "SIRECEIPT", "SIRECEIPTLINE", "TEMP_AGEING_POST_RECEIVED", "TEMP_AGEING_POST_TXN", "VW_BEA_NO_WRO")
    } else args_map("tables")
    //
    val table_keys: Map[String, String] = Map("AGEING_CBS" -> "JOURNALENTRY_ID", "AGEING_POST" -> "ACC_PERIOD", "AGEING_POST_NEW" -> "ACC_PERIOD", "AP_TEMP_BILLACCPERIOD" -> "MIN_ACC_PERIOD", "AP_TEMP_BILLNO" -> "JOURNALENTRYLINE_ID", "AP_TEMP_BOGUARANTORLETTER" -> "JOURNALENTRYLINE_ID", "CBMISPAYMENT" -> "CBMISPAYMENT_ID", "GL_LASTPOSTDATE" -> "ACC_PERIOD", "GL_OTHERPAYMENT" -> "GL_OTHERPAYMENT_ID", "GL_PA_TRANS_TYPE" -> "TRANS_REF", "GL_POST" -> "ACCOUNT_CODE", "GL_POST_FULL" -> "ACCOUNT_CODE", "GLCCMATCHPOST" -> "GLCCMATCHPOST_ID", "GLCCMATCHPOSTSUN" -> "ACCOUNT_CODE", "GNDEBTORTYPE_GLMAP" -> "GNDEBTORTYPE_GLMAP_ID", "GNPARAMETER" -> "PARAMETER_CODE", "GNPAYMENTMODE_GLMAP" -> "GNPAYMENTMODE_GLMAP_ID", "JOURNALENTRYLINE" -> "JOURNALENTRYLINE_ID", "SIOPENINGBALANCE" -> "GNSISTOCK_ID", "SIOPENINGBALANCE_P" -> "GNSISTOCK_ID", "SIRECEIPT" -> "SIRECEIPT_ID", "TEMP_AGEING_POST_RECEIVED" -> "ACC_PERIOD", "TEMP_AGEING_POST_TXN" -> "ACC_PERIOD", "VW_BEA_NO_WRO" -> "BOEPISODEACCOUNT_ID", "BBPATIENTBED" -> "BBPATIENTBED_ID", "BBPATIENTBEDHIST" -> "BBPATIENTBEDHIST_ID", "BOADJUSTMENT" -> "BOADJUSTMENT_ID", "BOCAREPROVIDERFEETXN" -> "BOCAREPROVIDERFEETXN_ID", "BOCHARGE" -> "BOCHARGE_ID", "BOCOLLECTION" -> "BOCOLLECTION_ID", "BODISCOUNT" -> "BODISCOUNT_ID", "BOEPISODEACCOUNT" -> "BOEPISODEACCOUNT_ID", "BOEPISODEACCOUNTTXN" -> "BOEPISODEACCOUNTTXN_ID", "BOEPISODEDEBTOR" -> "BOEPISODEDEBTOR_ID", "BOEPISODEDEBTORTXN" -> "BOEPISODEDEBTORTXN_ID", "BORECEIPT" -> "BORECEIPT_ID", "BOWRITEOFFTXN" -> "BOWRITEOFFTXN_ID", "CBCUSTOMER" -> "CBCUSTOMER_ID", "CBDISCOUNT" -> "CBDISCOUNT_ID", "CBRECEIPT" -> "CBRECEIPT_ID", "CBRECEIPTLINE" -> "CBRECEIPTLINE_ID", "CBREFUND" -> "CBREFUND_ID", "CBREFUNDLINE" -> "CBREFUNDLINE_ID", "CBRETURN" -> "CBRETURN_ID", "CBSALE" -> "CBSALE_ID", "CBSALELINE" -> "CBSALELINE_ID", "CBTXN" -> "CBTXN_ID", "FDADDRESS" -> "FDADDRESS_ID", "FDDIAGNOSIS" -> "FDDIAGNOSIS_ID", "FDDIAGNOSISLINE" -> "FDDIAGNOSISLINE_ID", "FDEPISODE" -> "FDEPISODE_ID", "FDPATIENT" -> "FDPATIENT_ID", "FDPATIENTCATEGORYHIST" -> "FDPATIENTCATEGORYHIST_ID", "FDPERSON" -> "FDPERSON_ID", "GLCCMATCHPOSTLINE" -> "GLCCMATCHPOSTLINE_ID", "GLCREDITCARD" -> "GLCREDITCARD_ID", "GLDAILYLOG" -> "GLDAILYLOG_ID", "GNACCOMMODATION" -> "GNACCOMMODATION_ID", "GNACCOMMODATIONCHG" -> "GNACCOMMODATIONCHG_ID", "GNBANK" -> "GNBANK_ID", "GNBED" -> "GNBED_ID", "GNBILLINGGRP" -> "GNBILLINGGRP_ID", "GNCAPPROFEE" -> "GNCAPPROFEE_ID", "GNCAREPROVIDER" -> "GNCAREPROVIDER_ID", "GNCAREPROVIDERFEE" -> "GNCAREPROVIDERFEE_ID", "GNCAREPROVIDERFEECLASS" -> "GNCAREPROVIDERFEECLASS_ID", "GNCAREPROVIDERFEELINE" -> "GNCAREPROVIDERFEELINE_ID", "GNCATEGORY" -> "GNCATEGORY_ID", "GNCHARGEITEM" -> "GNCHARGEITEM_ID", "GNCHARGEITEMLINE" -> "GNCHARGEITEMLINE_ID", "GNCODE" -> "GNCODE_ID", "GNCODELINE" -> "GNCODELINE_ID", "GNCONTROLPARAMETER" -> "HOSPITAL_CODE", "GNDISCOUNT" -> "GNDISCOUNT_ID", "GNGLACCOUNT" -> "GNGLACCOUNT_ID", "GNGLREVENUE" -> "GNGLREVENUE_ID", "GNICDCODE" -> "ICD_CODE", "GNMERGEPROFEE" -> "GNMERGEPROFEE_ID", "GNORDERITEM" -> "GNORDERITEM_ID", "GNORGANISATION" -> "GNORGANISATION_ID", "GNPATIENTCLASS" -> "GNPATIENTCLASS_ID", "GNPAYMENTMODETYPE" -> "PAYMENT_MODE", "GNPRORATEDPROFEE" -> "GNPRORATEDPROFEE_ID", "GNREVENUE" -> "GNREVENUE_ID", "GNROOM" -> "GNROOM_ID", "GNSIACCOUNT" -> "GNSIACCOUNT_ID", "GNSISTOCK" -> "GNSISTOCK_ID", "GNSISTOREROOM" -> "GNSISTOREROOM_ID", "GNSITXNTYPE" -> "GNSITXNTYPE_ID", "GNSUBCATEGORY" -> "GNSUBCATEGORY_ID", "GNTAX" -> "GNTAX_ID", "GNTAXLINE" -> "GNTAXLINE_ID", "GNTXNCODE" -> "GNTXNCODE_ID", "GNTXNTYPE" -> "GNTXNTYPE_ID", "GNUSER" -> "GNUSER_ID", "GNWARD" -> "GNWARD_ID", "JOURNALENTRY" -> "JOURNALENTRY_ID", "ODORDER" -> "ODORDER_ID", "ODORDERLINE" -> "ODORDERLINE_ID", "ODORDERLINEDETAIL" -> "ODORDERLINEDETAIL_ID", "PHDRUGORDER" -> "PHDRUGORDER_ID", "PHDRUGORDERLINE" -> "PHDRUGORDERLINE_ID", "PHDRUGPREPAREDISPENSE" -> "PHDRUGPREPAREDISPENSE_ID", "PHDRUGRETURN" -> "PHDRUGRETURN_ID", "SIMOVEMENT" -> "SIMOVEMENT_ID", "SIMOVEMENTLINE" -> "SIMOVEMENTLINE_ID", "SIPURCHASE" -> "SIPURCHASE_ID", "SIPURCHASELINE" -> "SIPURCHASELINE_ID", "SIRECEIPTLINE" -> "SIRECEIPTLINE_ID")
    println("starting job")
    //
    schemas.map(schema => {
      var table_counter: Int = 0
      tables.map(table => {
        //println(s"starting for $table ")
        table_counter += 1
        f_f(schema, table, table_keys, table_counter, prop, s3path, db_ip)
      })
    })
  }

    def f_f(schema: String, table: String, table_keys: Map[String, String], table_counter:Int, prop:Properties, s3path:String, db_ip:String): Future[Int] = Future {
      //if (table_counter == 1) println(java.util.Calendar.getInstance.getTime)
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
      //println("!!! inside f_f-2 " )
      val df_db_schema = spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select * from $schema.$table)",prop).schema
      //
      val table_key:String = table_keys(table)
      //println("!!! inside f_f-1 " + s"(select count(distinct($table_key)) from $schema.$table)")
      df_db_cnt = spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select count(*) from $schema.$table)",prop).first.mkString.toDouble.toInt
      df_db_cnt_dist_key = spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select count(distinct($table_key)) as cd_ from $schema.$table)",prop).first.mkString.toDouble.toInt
      df_db_sum_key = BigDecimal(spark.read.jdbc(s"jdbc:oracle:thin:@//$db_ip:1521/FSCLIVE", s"(select sum($table_key) as s_ from $schema.$table)",prop).first.mkString)
      //println("!!! inside f_f0 " )
      trying = Try(spark.read.option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/LOAD*"))
      df_base_cnt = 0
      df_dl_cnt_dist_key = 0
      //**** DF BASE
      trying match {
        case Success(v) => {
          df_base = spark.read.schema(df_db_schema).option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/LOAD*")
          df_base_cnt = df_base.count
          if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
            df_dl_cnt_dist_key = df_base.select(countDistinct(col(table_keys(table)))).first.mkString.toDouble.toInt
            df_dl_sum_key = BigDecimal(df_base.select(sum(col(table_keys(table)))).first.mkString)
          }

        }
        case Failure(e) =>
      }
      //println("!!! inside f_f1")
      //**** DF CDC
      trying = Try(spark.read.option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/2017*"))
      //df_cdc = spark.emptyDataFrame
      df_cdc_i_cnt = 0
      df_cdc_d_cnt = 0
      trying match {
        case Success(v) => {
          df_cdc = spark.read.schema(StructType(StructField("I_U_D", StringType, true) +: df_db_schema.fields)).option("escape","\"").option("multiLine","true").csv(s"$s3path/$schema/$table/2017*")
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
        case Failure(e) =>
      }
      //println("!!! inside f_f2")
      df_dl_cnt = df_base_cnt + df_cdc_i_cnt - df_cdc_d_cnt
      val df_dl_minus_db_cnt = df_dl_cnt - df_db_cnt
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
      println(s"**RowCount**,$schema,$table,," + "base+I-D=DL||DB," + df_base_cnt +",+" + df_cdc_i_cnt + ",-" + df_cdc_d_cnt + ",=" + df_dl_cnt + ",||," + df_db_cnt + ", \t\t >> ," + str_result_cnt + ",cnt_diff = " +df_dl_minus_db_cnt)
      if (table_keys(table).contains("_ID") && table != "SIOPENINGBALANCE" && table != "VW_BEA_NO_WRO" && table != "SIOPENINGBALANCE_P") {
        println(s"**DistinctKeyCount**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_cnt_dist_key + ",||," + df_db_cnt_dist_key + ", \t\t >> ," + str_result_cnt_dist_key  + ",dist_diff = " +df_dl_minus_db_cnt_dist_key)
        println(s"**sumKey**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_sum_key + ",||," + df_db_sum_key + ", \t\t >> ," + str_result_sum_key  + ",sum_diff = " +df_dl_minus_db_sum_key)
      } else {
        println(s"**DistinctKeyCount**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_cnt_dist_key + ",||," + df_db_cnt_dist_key + ", \t\t >> ," + "Warning: No PK detected")
        println(s"**sumKey**,$schema,$table,$table_key," + "DL||DB,,,," + df_dl_sum_key + ",||," + df_db_sum_key + ", \t\t >> ," + "Warning: No PK detected")
      }
      //println("!!! inside f_f3")
      if (table_counter == table_keys.size) println("**END****END****END****END**, "+println(java.util.Calendar.getInstance.getTime))
      1
    }


}
