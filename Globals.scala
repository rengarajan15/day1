package com.hcsc.datalake.spark.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.HdfsConfiguration

object Globals {

			val hadoopConf = new HdfsConfiguration()
			val hdfsConf = FileSystem.get(hadoopConf)

			val dateFormat = new SimpleDateFormat("YYYYMMdd")
			val dayFormat = new SimpleDateFormat("MMdd")
			val timeFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSSSSS")

			var configMap   = Map[String,Any]()
			var schemaMap   = List[Map[String,Any]]()
			var colList     = List[String]()
			var alldirBf    = ListBuffer[Map[String,String]]()
			var logOvwrtInd = true

			def getCurrentDate() =
		  { dateFormat.format(Calendar.getInstance().getTime()) }

	    def getCurrentTime() =
   		{ timeFormat.format(Calendar.getInstance().getTime()) }

	    def getCurrentDay() =
	  	{ dayFormat.format(Calendar.getInstance().getTime()) }

}
