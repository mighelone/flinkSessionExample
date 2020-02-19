/*
 * Example from http://blog.madhukaraphatak.com/introduction-to-flink-streaming-part-7/
 * https://github.com/phatak-dev/flink-examples/blob/master/src/main/scala/com/madhukaraphatak/flink/streaming/examples/sessionwindow/SessionTrigger.scala
 */

package com.mvasce

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{PurgingTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}

import scala.util.Try

object SessionWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = env.socketTextStream("localhost", 9000)

    val values = source.map(
      value => {
        val columns = value.split(",")
        val endOfSignal = Try(Some(columns(2))).getOrElse(None)
        Session(columns(0).toInt, columns(1).toDouble, endOfSignal)
      }
    )
      .keyBy("sessionId")
      .window(GlobalWindows.create())
      .trigger(PurgingTrigger.of(new SessionTrigger[GlobalWindow]()))
      .sum("value")

    values.print()

    env.execute("SessionWindowExample")
  }

  case class Session(sessionId: Int, value: Double, endSignal: Option[String])

  class SessionTrigger[W <: Window] extends Trigger[Session, Window] {
    override def onElement(element: Session, timestamp: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {
      if (element.endSignal.isDefined) TriggerResult.FIRE
      else TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: Window, ctx: Trigger.TriggerContext): Unit = {
    }
  }


}
