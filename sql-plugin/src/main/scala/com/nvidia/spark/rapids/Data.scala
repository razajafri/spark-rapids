package com.nvidia.spark.rapids

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.util.TaskCompletionListener

object Data extends SparkListener with TaskCompletionListener {
  val datas = new ArrayBuffer[Data]

  def add(d: Data): Unit = {
    synchronized {
      datas += d
    }
  }

  override def onApplicationEnd(sparkListener: SparkListenerApplicationEnd): Unit = {
    write
  }

  private def write = {
    synchronized {
      val writeTotal = datas.filter(d => d.mode.equalsIgnoreCase("write")).map(_.time).sum
      val readTotal = datas.filter(d => d.mode.equalsIgnoreCase("read")).map(_.time).sum
      if (writeTotal > 0 || readTotal > 0) {
        val file = new File("/home/rjafri/cache-perf.txt")
        val bw = new BufferedWriter(new FileWriter(file, true))
        if (writeTotal > 0) {
          bw.append("write: " + writeTotal.toString)
          bw.append("\n")
        }
        if (readTotal > 0) {
          bw.append("read: " + readTotal.toString)
          bw.append("\n")
        }
        bw.close()
      }
      datas.clear
    }
  }

  override def onTaskCompletion(context: TaskContext): Unit = {
    write
  }
}

class Data(val mode: String, val time: Long, ser: String, acc: String) extends {
  override def toString(): String = {
    s"\nSerializer, ${ser}\nmode: ${mode}\nser: ${ser}\nacc: ${acc}\ntime: ${time}"
  }
}