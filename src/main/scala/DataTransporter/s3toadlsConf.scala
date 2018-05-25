package DataTransporter

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

/**
  * Created by depatel on 5/24/18.
  */
class s3toadlsConf(args: Seq[String]) extends ScallopConf(args) {

  val s3path = opt[String](name="s3path", required = true, noshort = false, descr = "s3 input file path")
  val s3accessKey = opt[String](name="s3accessKey", required = true, noshort = false, descr = "s3 access key")
  val s3secretKey = opt[String](name="s3secretKey", required = true, noshort = false, descr = "s3 secret key")
  val adls = opt[String](name="adlsName", required = true, noshort = false, descr = "azure adls uri")
  val tenantId = opt[String](name="tenantId", required = true, noshort = false, descr = "azure tenant id")
  val spnClientId = opt[String](name="spnClientId", required = true, noshort = false, descr = "azure service principle client id")
  val spnClientSecret = opt[String](name="spnClientSecret", required = true, noshort = false, descr = "azure service principle client secret")
  val adlsOutputPath = opt[String](name="adlsOutputPath", required = true, noshort = false, descr = "adls output path")
  val writeMode = opt[String](name="writeMode", required = true, noshort = false, descr = "writemode: overwrite(default) | append")
  val inputFormat = opt[String](name="inputFormat", required = true, noshort = false, descr = "input file format")
  val outputFormat = opt[String](name="outputFormat", required = true, noshort = false, descr = "output file format")
  val inputOptions = opt[String](name="inputOptions", required = false, noshort = false, descr = "input options for reading data")
  val outputOptions = opt[String](name="outputOptions", required = false, noshort = false, descr = "output options for writing data")
  val job_run_id = opt[Long](name="job_run_id", required = false, noshort = false, descr = "job execution id")
  val execution_date = opt[String](name="execution_date", required = false, noshort = false, descr = "job execution date")
  val prev_exec_date = opt[String](name="prev_exec_date", required = false, noshort = false, descr = "previous job execution date")


  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
  }

  verify()
}
