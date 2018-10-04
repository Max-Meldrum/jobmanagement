package executor.yarn

import java.io.{BufferedOutputStream, FileOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.yarn.conf.YarnConfiguration

private[executor] object ExecutorUtils extends LazyLogging {


  /** Writes binary to local filesystem from HDFS
    *
    * @param src HDFS path to the binary
    * @param local local path on the filesystem (in the apps's ExecutionEnvironment)
    * @return true on success, otherwise false
    */
  def moveToLocal(src: String, local: String): Boolean = {
    try {
      val conf = new YarnConfiguration()
      val fs = FileSystem.get(conf)
      val srcPath = new Path(src)
      val inputStream = fs.open(srcPath)
      val outputStream = new BufferedOutputStream(new FileOutputStream(local))
      logger.info(s"Moving $srcPath to $local")
      IOUtils.copyBytes(inputStream, outputStream, conf)
      true
    } catch {
      case err: Exception =>
        logger.error("Failed to move binary to local filesystem with error: " + err.toString)
        false
    }
  }

}
