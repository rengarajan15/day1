
package com.hcsc.datalake.spark.controller

import com.hcsc.datalake.spark.utils.{AppUtil,Util,Globals}

object Driver extends Util {

	def main(args: Array[String]) = {

					var mode             = ""
					var mapFile          = ""
					var schemaFile       = ""
					var configFile       = ""
					var sessionName      = ""
					var srcPath          = ""
					var hist_start_date  = ""
					var hist_end_date    = ""
// Pass below arguments for the spark submit in order
//					1. Mode of Spark execution - Local/cluster
//          2. mapFile - CSV for user to enter source and target path
//					3. SchemaFile - CSV for user to enter List of Source columns
//				  4. SessionName - Name for spark session 
					
					if(args.size == 7) {
						    mode            = args(0)
								schemaFile      = args(1)
								configFile      = args(2)
								srcPath         = args(3)
								sessionName     = args(4)
								hist_start_date = args(5)
								hist_end_date   = args(6)

								logger.info("Starting the job")
								logger.info("==================================================")
								logger.info("Running mode : "+ mode)
								//logger.info("Mapping file : "+ mapFile)
								logger.info("Schema File  : "+ schemaFile)
								logger.info("Config File  : "+ configFile)
								logger.info("Spark session name : "+ sessionName)
								logger.info("Source path : "+ srcPath)
								logger.info("==================================================")
					} else
					{
						logger.error("Please provide all the required arguments")
						System.exit(1)
					}			

			try {

				    val sparkSes = getSession(mode, sessionName)
			    	logger.info("### Spark session created successfully ###")

			    	AppUtil.LoadConfigFile(sparkSes,configFile)
			    	logger.info("### Config File Successfully Loaded ###")
			    	   
				    val alldirBf = AppUtil.RetriveFilePath(sparkSes,srcPath,hist_start_date,hist_end_date)
				    alldirBf.foreach(println)
			    	logger.info("### Mapping file processed and List[Key,Value] built successfully ###")
			    	
						val schema = AppUtil.BuildSchema(sparkSes,schemaFile)
						logger.info("### Schema created successfully ###")

						AppUtil.writeTgtDest(sparkSes,alldirBf,schema)
						logger.info("### Data inserted to target table successfully ###")

			    } catch
			    {
			    case e:Exception=>
			      {
				    writeToLog(e,"RED")
				    println(e)
				    throw e
			      }
			    } finally {}
	}
}
