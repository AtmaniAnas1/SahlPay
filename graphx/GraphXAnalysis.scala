// Partie 6 — Analyse GraphX (Scala / API RDD) du graphe exporté par part6_graphframes.py
//
// Lancement (dans le conteneur jupyter) :
//   spark-shell --master spark://spark-master:7077 -i /opt/project/graphx/GraphXAnalysis.scala
//
// Entrées  : lake/gold/graph_model/{vertices,edges}  (Parquet : id/type, src/dst/relationship/tx_count/total_amount)
// Sorties  : lake/gold/graph_model/vertex_ids        (correspondance id texte -> VertexId Long)
//            lake/gold/graph_compare/graphx_degrees  (degrés du sous-graphe des transferts)
//            lake/gold/graphx/{components,pagerank}
//            outputs/part6_graphx_timings.json

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

import spark.implicits._

spark.sparkContext.setLogLevel("WARN")
val lake = sys.env.getOrElse("SAHLPAY_LAKE", "s3a://lake").stripSuffix("/")
val outputs = sys.env.getOrElse("SAHLPAY_OUTPUTS", "/opt/project/outputs")
val timings = scala.collection.mutable.LinkedHashMap[String, Double]()

def timed[T](label: String)(body: => T): T = {
  val start = System.nanoTime()
  val result = body
  val seconds = (System.nanoTime() - start) / 1e9
  timings(label) = math.round(seconds * 1000) / 1000.0
  println(f"[TEMPS] $label : $seconds%.3f s")
  result
}

def section(title: String): Unit = println("\n" + "=" * 100 + s"\n$title\n" + "=" * 100)

case class EdgeAttr(relationship: String, txCount: Long, totalAmount: Double)

section("1. Chargement des tables Parquet et attribution d'un VertexId (Long) unique")
val verticesDF = spark.read.parquet(s"$lake/gold/graph_model/vertices")
val edgesDF = spark.read.parquet(s"$lake/gold/graph_model/edges")

// rang de l'identifiant texte trié : unique et stable d'une exécution à l'autre
val idMap = timed("gx_vertex_ids") {
  val m = verticesDF.select("id", "type").orderBy("id").rdd.zipWithIndex()
    .map { case (row, idx) => (row.getString(0), row.getString(1), idx) }
    .toDF("id", "type", "vid").cache()
  m.count()
  m
}
idMap.write.mode("overwrite").parquet(s"$lake/gold/graph_model/vertex_ids")
idMap.show(5)

val vertexRDD: RDD[(VertexId, (String, String))] =
  idMap.rdd.map(r => (r.getLong(2), (r.getString(0), r.getString(1))))
val edgeRDD: RDD[Edge[EdgeAttr]] = edgesDF
  .join(idMap.select($"id".as("src"), $"vid".as("srcId")), "src")
  .join(idMap.select($"id".as("dst"), $"vid".as("dstId")), "dst")
  .select("srcId", "dstId", "relationship", "tx_count", "total_amount")
  .rdd.map(r => Edge(r.getLong(0), r.getLong(1), EdgeAttr(r.getString(2), r.getLong(3), r.getDouble(4))))

val graph = timed("gx_build_graph") {
  val g = Graph(vertexRDD, edgeRDD).cache()
  g.vertices.count(); g.edges.count()
  g
}
println(s"Graphe complet : ${graph.vertices.count()} sommets, ${graph.edges.count()} aretes")
graph.vertices.map(_._2._2).countByValue().toSeq.sortBy(_._1).foreach { case (t, n) => println(f"  sommets $t%-14s $n") }
graph.edges.map(_.attr.relationship).countByValue().toSeq.sortBy(_._1).foreach { case (t, n) => println(f"  aretes  $t%-14s $n") }

section("2. Sous-graphe des transferts (wallet -TRANSFERS_TO-> wallet) : degrees")
val transfers = graph.subgraph(
  epred = t => t.attr.relationship == "TRANSFERS_TO",
  vpred = (_, attr) => attr._2 == "wallet").cache()
println(s"Sous-graphe : ${transfers.vertices.count()} comptes, ${transfers.edges.count()} aretes de transfert")

val degreesDF = timed("gx_degrees") {
  val d = transfers.degrees
    .leftOuterJoin(transfers.inDegrees)
    .leftOuterJoin(transfers.outDegrees)
    .map { case (vid, ((deg, in), out)) => (vid, deg, in.getOrElse(0), out.getOrElse(0)) }
    .toDF("vid", "degree", "inDegree", "outDegree")
    .join(idMap.select("vid", "id"), "vid")
    .select("id", "degree", "inDegree", "outDegree")
    .cache()
  d.count()
  d
}
degreesDF.orderBy(desc("inDegree"), $"id").show(10)
degreesDF.write.mode("overwrite").parquet(s"$lake/gold/graph_compare/graphx_degrees")

section("3. connectedComponents (comptes reliés par au moins un transfert)")
val components = timed("gx_connected_components") {
  val c = transfers.connectedComponents().vertices
    .innerJoin(transfers.degrees) { case (_, component, _) => component }   // ignore les comptes isolés
    .toDF("vid", "component")
    .join(idMap.select("vid", "id"), "vid")
    .cache()
  c.count()
  c
}
val sizes = components.groupBy("component").agg(count("*").as("taille")).orderBy(desc("taille"))
sizes.show(10)
println(s"${sizes.count()} composantes ; distribution des tailles :")
sizes.groupBy("taille").count().orderBy("taille").show(50)
components.join(sizes, "component").write.mode("overwrite").parquet(s"$lake/gold/graphx/components")

section("4. pageRank (tolérance 0.0001) : centralité des comptes dans le graphe des transferts")
val pagerank = timed("gx_pagerank") {
  val p = transfers.pageRank(0.0001).vertices
    .toDF("vid", "pagerank")
    .join(idMap.select("vid", "id"), "vid")
    .join(degreesDF, Seq("id"), "left")
    .select("id", "pagerank", "degree", "inDegree", "outDegree")
    .cache()
  p.count()
  p
}
pagerank.orderBy(desc("pagerank")).show(15)
pagerank.write.mode("overwrite").parquet(s"$lake/gold/graphx/pagerank")

section("5. Temps d'exécution GraphX")
Files.createDirectories(Paths.get(outputs))
val json = timings.map { case (k, v) => s"""  "$k": $v""" }.mkString("{\n", ",\n", "\n}\n")
Files.write(Paths.get(outputs, "part6_graphx_timings.json"), json.getBytes(StandardCharsets.UTF_8))
print(json)

spark.stop()
System.exit(0)
