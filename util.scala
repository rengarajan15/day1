
package com.hcsc.datalake.spark.utils

import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait Util {
	    @transient lazy val logger: Logger = LogManager.getLogger("Data Mover")

			def getSession(mode: String, app: String): SparkSession={
					println("****************coming inside getSession***************")
					return SparkSession
							.builder
							.appName(app)
							.master(mode)
							.config("hive.exec.dynamic.partition", "true")
							.config("hive.exec.dynamic.partition.mode", "nonstrict")
							.enableHiveSupport()
							.getOrCreate()
	}

	    def writeToLog(flowout:Any,logtype:String)={
		      println("Coming inside writeToLog")
		      logtype match {                                 
		      case "GREEN" => logger.warn(flowout)
		      case "RED"   => 
		        {
		         logger.error(flowout)
			       logger.error("Exiting the process due to above invalid Processing!!")    
			       System.exit(1)
			      }    
		      case _       => logger.error("Invalid Logging passed")
		}
	}

}
