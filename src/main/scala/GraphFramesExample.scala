import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
import org.joda.time.DateTime

object GraphFramesExample {
  private val src = "src"
  private val dst = "dst"
  private val month = "month"
  private val dayOfMonth = "dayOfMonth"
  private val dayOfWeek = "dayOfWeek"
  private val distance = "distance"
  private val date = "date"

  @transient lazy val sparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("SparkGraphFrames")
      .config("spark.master", "local")
      .getOrCreate()
  import sparkSession.implicits._

  def main(args:Array[String]): Unit = {
    sparkSession.sparkContext.setLogLevel("WARN")


    println("Empezamos!")
    val flights = sparkSession.read.format("csv").option("path", "/home/evl/Escritorio/TFG/dataexpo_routes/test.csv").option("header", "true").option("inferSchema", "true").load


    println("vamos a crear el grafo!")
    val graph:GraphFrame = createGraph(flights).cache()

        println("Number of aiports")
        println(getNumOfAiports(graph))

        println("Number of flights")
        println(getNumOfFlights(graph))

        println("aiports")
        println(getAiports(graph))

        println("In degree sorted vertex")
        getInDegreeSorted(graph)

                println("Out degree sorted vertex")
        getOutDegreeSorted(graph)

        println("Degree ratio sorted")
        getRatioDegreeSorted(graph)

        println("Label propagation")
            getLabelPropagation(graph)

            println("Strongly connected components")
    getStronglyConnectedComponents(graph)


    println("Flights whit more than 500 miles")
    getFilteredVertex(graph, (x: Int) => x>500, distance, true)


    //val triplets = graph.triplets

//    val shortest = graph.shortestPaths.landmarks(Seq("a", "d")).run()
//    shortest.select("id", "distances").limit(5).show()





    val triangles = getMinDistTriangles(graph)
      triangles.show()

  }

  def getMinDistTriangles(graph: GraphFrame): Dataset[Row] = {
    val triangles = graph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
    triangles.select("*").where("a.id != b.id").where("b.id != c.id")
      .where("ab.date < bc.date").where("bc.date < ca.date")
    //      .orderBy("ab.distance + bc.distance + ca.distance")
    //      .orderBy(exp("ab.distance + bc.distance + ca.distance"))
  }

  def getStronglyConnectedComponents(graph: GraphFrame, maxIter: Int = 5, ascending: Boolean = false, sample: Boolean = true): Dataset[Row] = {
    val scc =graph.stronglyConnectedComponents.maxIter(maxIter).run()
    if (ascending) {
      val output = scc.orderBy($"component".asc)
      if (sample) output.limit(5).show()
      output
    }
    else {
      val output = scc.orderBy($"component".desc)
      if (sample) output.limit(5).show()
      output
    }
  }
  def getLabelPropagation(graph: GraphFrame, ascending: Boolean = false, sample: Boolean = true): Dataset[Row] = {
    val lp = graph.labelPropagation.maxIter(5).run()
    if (ascending) {
      val output = lp.orderBy($"label".asc)
      if (sample) output.limit(5).show()
      output
    }
    else {
      val output = lp.orderBy($"label".desc)
      if (sample) output.limit(5).show()
      output
    }
  }

  def getInDegreeSorted(graph: GraphFrame, sorted: Boolean = true, sample: Boolean = true): Dataset[Row] = {
    val inD =
      if (sorted) graph.inDegrees.sort($"inDegree".desc)
      else graph.inDegrees

    if (sample) inD.limit(5).show
    inD
  }
  def getOutDegreeSorted(graph: GraphFrame, sorted: Boolean = true, sample: Boolean = true): Dataset[Row] = {
    val outD =
      if (sorted) {graph.outDegrees.sort($"outDegree".desc)}
      else{graph.outDegrees}

    if (sample) outD.limit(5).show
    outD
  }

  def getRatioDegreeSorted(graph: GraphFrame,ascending: Boolean = false, sample: Boolean = true): Dataset[Row] = {
    val inD = getInDegreeSorted(graph, sorted = false, sample = false)
    val outD = getOutDegreeSorted(graph, sorted = false, sample = false)

    val ratio = inD.join(outD, inD.col("id") === outD.col("id"))
      .drop(outD.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

    if (ascending) {
      val output = ratio.orderBy($"degreeRatio".desc)
      if (sample) output.limit(10).show()
      output
    }

    else {
      val output = ratio.orderBy($"degreeRatio".asc)
      if (sample) output.limit(10).show()
      output
    }
  }
  def getFilteredVertex[T](g: GraphFrame, condition: T => Boolean, col: String, sample: Boolean = false): Dataset[Row] = {
    val filtered = g.edges.filter(row => condition(row.getAs(col)))

    if (sample) filtered.limit(2).show()
    filtered
  }




  def createGraph(flightsDS: DataFrame): GraphFrame = {

    val dayOfYear = (year:Int, month:Int, day:Int) => new DateTime().year.setCopy(year).monthOfYear.setCopy(month).dayOfMonth.setCopy(day).dayOfYear().get
    val fFiltered = flightsDS.map(flightRow => {
      val dst = flightRow.getAs[String]("Dest")
      val distance = flightRow.getAs[Int]("Distance")
      val src = flightRow.getAs[String]("Origin")
      val year = flightRow.getAs[Int]("Year")
      val month = flightRow.getAs[Int]("Month")
      val dayOfWeek = flightRow.getAs[Int]("DayOfWeek")
      val date = dayOfYear(year, month, dayOfWeek)
      (src, dst, date, distance)})



    val colNames = Seq(src, dst, date, distance)
    val aiports = fFiltered.rdd.flatMap{case (source, dest, _,_) => Seq(source, dest)}.distinct()
    val edg = fFiltered.toDF(colNames: _*)
    val vertexID = aiports.distinct().toDF("id")


    println("vertices")
    vertexID.printSchema()
    println("esquema de las aristas")
    edg.printSchema()
    GraphFrame(vertexID, edg)
  }


  def getNumOfAiports(graph: GraphFrame): Long ={
    graph.vertices.count()
  }
  def getNumOfFlights(graph: GraphFrame): Long ={
    graph.edges.count()
  }

  def getAiports(graph: GraphFrame): String ={
    graph.vertices.columns.toString
  }

}

