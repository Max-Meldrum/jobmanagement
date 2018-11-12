package runtime.appmanager.core.util

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl

private[runtime] object DagDrawer {

  // Just a Prototype for now
  def logicalDAG(): String = {
    val graph = Graph(
      vertices = Set(
        "source", "map", "filter", "flatmap", "reduce", "sink"
      ),
      edges = List(
        "source" → "map",
        "map" → "filter",
        "filter" → "flatmap",
        "flatmap" → "reduce",
        "reduce" → "sink"
      )
    )
    val prefs = LayoutPrefsImpl(vertical = false, elevateEdges = false)
    GraphLayout.renderGraph(graph, layoutPrefs = prefs)
  }

  def phyiscalDAG(): String = {
    val source = """Source
               				 |────────────
               				 |name = Kafka""".stripMargin
    val t1 = """Task
               				 |────────────
               				 |name = map
               				 |host = 2G, 2 Cores
               				 |status = deploying""".stripMargin
    val t2 = """Task
               				 |────────────
               				 |name = filter
               				 |host = 4G, 2 Cores
               				 |status = running""".stripMargin
    val t3 = """Task
               				 |────────────
               				 |name = unifier
               				 |host = 1G, 2 Cores
               				 |status = running""".stripMargin
    val sink = """Sink
               				 |────────────
               				 |name = Kafka""".stripMargin
    val vertices4 = Set(source, t1, t2, t3, sink)
    val edges4 = List(source → t1, source → t2, t1 → t3, t2 →  t3, t3 → sink)

    val graph4 = Graph(vertices4, edges4)
    val prefs = LayoutPrefsImpl(vertical = false, elevateEdges = false)
    GraphLayout.renderGraph(graph4, layoutPrefs = prefs)
  }
}
