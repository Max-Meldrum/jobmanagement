package runtime.appmanager.core.util

import java.awt.Font
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.RenderingHints
import java.awt.image.BufferedImage

import akka.actor.ActorRef
import runtime.appmanager.core.actor.AppMaster.{AppEvent, PrintEvent}

import scala.collection.mutable.ArrayBuffer



// Based on https://github.com/eugenp/tutorials/blob/master/core-java/src/main/java/com/baeldung/asciiart/AsciiArt.java
// MIT License
private[runtime] object Ascii {

  sealed trait DAG
  case object PhysicalDAG extends DAG
  case object LogicalDAG extends DAG
  final case class Settings(font: Font, width: Int, height: Int)

  def draw(text: String, artChar: String, spacing: Int = 0, settings: Settings): ArrayBuffer[String] = {
    val image = getImageIntegerMode(settings.width, settings.height)
    val graphics2D = getGraphics2D(image.getGraphics, settings)
    graphics2D.drawString(text, spacing, (settings.height * 0.67).toInt)

    val buffer = ArrayBuffer.empty[String]

    for (y <- 0 until settings.height) {
      val builder = new StringBuilder()
      for (x <- 1 until settings.width) {
        val str = if (image.getRGB(x, y) == -16777216) " " else artChar
        builder.append(str)
      }
      if (!builder.toString.trim.isEmpty)
        buffer += builder.toString()
    }
    buffer
  }

  private def getImageIntegerMode(width: Int, height: Int): BufferedImage = {
    new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
  }

  private def getGraphics2D(graphics: Graphics, settings: Settings): Graphics2D = {
    graphics.setFont(settings.font)
    val graphics2D = graphics.asInstanceOf[Graphics2D]
    graphics2D.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
    graphics2D
  }

  def createHeader(appmaster: ActorRef): Unit = {
    val arr = draw("CDA", "#", 0, AsciiSettings(width = 100, height = 20, fontSize = 13, font = Garamond))
    arr.foreach(line => appmaster ! PrintEvent(AppEvent(line)))
    appmaster ! PrintEvent(AppEvent("\n"))
  }

  def createDAGHeader(dag: DAG, appmaster: ActorRef): Unit = dag match {
    case PhysicalDAG =>
      val arr = draw("Physical DAG", "@", 0, AsciiSettings(width = 100, height = 20, fontSize = 13, font = Garamond))
      arr.foreach(line => appmaster ! PrintEvent(AppEvent(line)))
      appmaster ! PrintEvent(AppEvent("\n"))
    case LogicalDAG =>
      val arr = draw("Logical DAG", "@", 0, AsciiSettings(width = 100, height = 20, fontSize = 13, font = Garamond))
      arr.foreach(line => appmaster ! PrintEvent(AppEvent(line)))
      appmaster ! PrintEvent(AppEvent("\n"))
  }

}
