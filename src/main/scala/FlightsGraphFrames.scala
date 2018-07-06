import org.apache.spark.sql._
import org.graphframes._
import org.joda.time.DateTime

object FlightsGraphFrames {
  private val origin = "Origin"
  private val dest = "Dest"
  private val src = "src"
  private val dst = "dst"
  val date_year = "Year"
  val date_month = "Month"
  private val date_day_of_week = "DayOfWeek"
  private val miles = "Distance"
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
    val path =  "~/data/graphFramesFlights.csv"
    val flights = sparkSession.read.format("csv").
      option("path", path).option("header", "true").option("inferSchema", "true").load


    println("Vamos a crear el grafo!")
    val graph:GraphFrame = createGraph(flights).cache()

    println("Cantidad de aeropuertos:")
    println(getNumOfAiports(graph))

    println("Cantidad de vuelos:")
    println(getNumOfFlights(graph))

    println("Aeropuertos:")
    println(getAiports(graph))

    println("Aeropuertos ordenados en función de los vuelos que han aterrizado en ellos:")
    getInDegreeSorted(graph)

    println("Aeropuertos ordenados en función de los vuelos que han despegado en ellos:")
    getOutDegreeSorted(graph)

    println("Aeropuertos ordenados en función del ratio de vuelos aterrizados y despegados:")
    getRatioDegreeSorted(graph)

    println("Label propagation:")
    getLabelPropagation(graph)

    println("Componentes fuertemente conexas:")
    getStronglyConnectedComponents(graph)


    println("Vuelos que han recorrido más de 500 millas:")
    getFilteredVertex(graph, (x: Int) => x>500, miles, sample = true)

    println("Rutas que forman un triangulo")
    val triangles = getMinDistTriangles(graph)
      triangles.show(5)

  }

  /**
  Returns Dataset of triangles in the graph.
    */
  def getMinDistTriangles(graph: GraphFrame): Dataset[Row] = {
    val triangles = graph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
    triangles.select("*").where("a.id != b.id").where("b.id != c.id")
      .where("ab.date < bc.date").where("bc.date < ca.date")
  }

  /**
  Returns a dataset of aiports and column "rank" from computes the PageRank of aiports from an input graph
    */
  def getPageRankSorted(graph: GraphFrame, resetProbability: Double = 0.15,
                        ascending: Boolean = false, sample: Boolean = true):  Dataset[Row]={

    val ranks = graph.pageRank.resetProbability(resetProbability).maxIter(10).run()

    if (ascending) {
      val output = ranks.vertices.orderBy($"pagerank".asc).select("id", "pagerank")
      if (sample) output.limit(5).show()
      output
    }
    else {
      val output = ranks.vertices.orderBy($"pagerank".desc).select("id", "pagerank")
      if (sample) output.limit(5).show()
      output
    }
  }

  /**
  Returns a dataset of aiports and column "component" from computes the
  stronglyConnectedComponents of aiports from an input graph
    */
  def getStronglyConnectedComponents(graph: GraphFrame, maxIter: Int = 5,
                                     ascending: Boolean = false, sample: Boolean = true): Dataset[Row] = {
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

  /**
  Returns a dataset of aiports and column "label" from computes the
  labelPropagation of aiports from an input graph
    */
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

  /**
  Returns Dataset of aiports and number of landings from the airport.
    */
  def getInDegreeSorted(graph: GraphFrame, sorted: Boolean = true, sample: Boolean = true): Dataset[Row] = {
    val inD =
      if (sorted) graph.inDegrees.sort($"inDegree".desc)
      else graph.inDegrees

    if (sample) inD.limit(5).show
    inD
  }

  /**
  Returns Dataset of aiports and number of takeoffs from the airport.
    */
  def getOutDegreeSorted(graph: GraphFrame, sorted: Boolean = true, sample: Boolean = true): Dataset[Row] = {
    val outD =
      if (sorted) {graph.outDegrees.sort($"outDegree".desc)}
      else{graph.outDegrees}

    if (sample) outD.limit(5).show
    outD
  }

  /**
  Returns quotient inDegree between outDegree of aiports sorted by quotient.
    */
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

  /**
  Returns Dataset of edges filtered by imput condition.
    */
  def getFilteredVertex[T](g: GraphFrame, condition: T => Boolean,
                           col: String, sample: Boolean = false): Dataset[Row] = {
    val filtered = g.edges.filter(row => condition(row.getAs(col)))

    if (sample) filtered.limit(2).show()
    filtered
  }

  /**
  Returns the number of aiports.
    */
  def getNumOfAiports(graph: GraphFrame): Long ={
    graph.vertices.count()
  }

  /**
  Returns the number of flights.
    */
  def getNumOfFlights(graph: GraphFrame): Long ={
    graph.edges.count()
  }

  /**
  Returns the aiports.
    */
  def getAiports(graph: GraphFrame)={
    graph.vertices.collectAsList()
  }

  /**
  Returns a graph whose vertices are airports and edges are flights between airports,
  with their distance traveled and date expressed on the day of the year.
    */
  def createGraph(flightsDF: DataFrame): GraphFrame = {

    val dayOfYear = (year:Int, month:Int, day:Int) =>
      new DateTime().year.setCopy(year).monthOfYear.setCopy(month).dayOfMonth.setCopy(day).dayOfYear().get

    val flightsDS = flightsDF.map(flightRow => {
      val source = flightRow.getAs[String](origin)
      val destination = flightRow.getAs[String](dest)
      val distance = flightRow.getAs[Int](miles)
      val year = flightRow.getAs[Int](date_year)
      val month = flightRow.getAs[Int](date_month)
      val dayOfWeek = flightRow.getAs[Int](date_day_of_week)
      (source, destination, date, distance)})

    val colNames = Seq(src, dst, date, miles)
    val aiports = flightsDS.rdd.flatMap{case (source, destination, _,_) => Seq(source, destination)}.distinct()
    val edg = flightsDS.toDF(colNames: _*)
    val vertexID = aiports.distinct().toDF("id")

    GraphFrame(vertexID, edg)
  }
}

