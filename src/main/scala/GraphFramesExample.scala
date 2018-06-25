
import org.apache.spark.sql._
import org.apache.spark.sql.functions.exp
import org.apache.spark
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

    val sqlContext = flights.sqlContext
    //import sqlContext.implicits._


    println("vamos a crear el grafo!")
    val graph:GraphFrame = createGraph(flights).cache()
//
//        println("cantidad de aeropuertos")
//        println(getNumOfAiports(graph))
//
//        println("cantidad de flights")
//        println(getNumOfFlights(graph))
//
//        println("aeropuertos")
//        println(getAiports(graph))
//
//        println("In degree sorted vertex")
//        getInDegreeSorted(graph)
//
//                println("Out degree sorted vertex")
//        getOutDegreeSorted(graph)
//
//        println("Degree ratio sorted")
//        getRatioDegreeSorted(graph)

    /*
        println("Label propagation")
            getLabelPropagation(graph)

            println("Strongly connected components")
    getStronglyConnectedComponents(graph)

        */
//
//    getFilteredVertex(graph, (x: Int) => x>500, distance, true)
//
//      //    graph.find("(a) - [e] -> (b)").show
//

//





    //val triplets = graph.triplets


//    val shortest = graph.shortestPaths.landmarks(Seq("a", "d")).run()
//    shortest.select("id", "distances").limit(5).show()


/*
    /** En graphframe ya no es obligatorio utilizar un VertexId como tipo Long.
      * i.e puedes identificar los vertices como te de la gana.
      * Y tampoco es obligatorio tener separado un VertexId y un VertexProperty,
      * lo que si es obligatorio es que el primer campo del DF de Vertices se identifique como id
      */
    val vertexArray = Array(
      ("1", "Alice", 28),
      ("2", "Bob", 27),
      ("3", "Charlie", 65),
      ("4", "David", 42),
      ("5", "Ed", 55),
      ("6", "Fran", 50)
    )
    /** Observacion: Los 2 primeros valores de la tupla son los identificadores de los vertices.
      * Observar que ya no hay que usar la clase Edge.
      * lo que si es obligatorio es que el DF de edges tenga los campos src y dst como identificadores
      * de los vertices origen y destino respectivamente.
      */
    val edgeArray = Array(
      ("2", "1", 7),
      ("2", "4", 2),
      ("3", "2", 4),
      ("3", "6", 3),
      ("4", "1", 1),
      ("5", "2", 2),
      ("5", "3", 8),
      ("5", "6", 3)
    )

    /**
      * Hasta ahora solo tenemos 2 arrays. Tenemos que convertirlos en 2 DataFrames y juntarlos en un GraphFrame
      */
    println("\n[Carga del grafo]")
    val g = getGraphFrame(vertexArray, edgeArray)

    /**
      * Diferentes formas de filtrar los vertices de un grafo
      *   showGreaterThan --> Se le pasa una edad fija (menos reutilizable)
      *   showFilteredVertex --> Se le pasa una funcion: Int => Boolean (mucho mas reutilizable)
      */the	user	experience	gains	greatly
outweigh	this	minor	overhead
    println("\n[Mayores que 40]")
    //showGreaterThan(g,40)

    val greatherThan40: Int => Boolean = (x: Int) => {x > 40}
    //showFilteredVertex(g, greatherThan40)
    // Tambien valdria --> showFilteredVertex(g, (x:Int) => x > 40)

    /**
      * Para mostrar los triplets:
      * OPCIONES:
      *   1. showJoinTriplets --> Utilizar joins entre vertices y aristas (no utilizas la potencia de GraphFrames)
      *   2. showTripletsDSL --> Utilizando Motif Finding, una DSL de grafos (muy potente)
      *   3. Ejecutar g.triplets, pero en realidad ejecuta g.find((src)-(e)->(dst))
      */
    //println("\n[Mostrar Triplets]")
    //showJoinTriplets(g)
    //showTripletsDSL(g)


    /**
      * Uno de los algoritmos mas utiles y usados es el pageRank --> Importancia de la pagina/vertice dentro de la red.
      */
    println("\n[PageRank de users]")
    pageRankUserGraph(g)
    */




    //          val inDeg = graph.inDegrees
    //          inDeg.sort($"inDegree".desc).limit(10).show
    //
    //          val outDeg = graph.outDegrees
    //          outDeg.sort($"outDegree".desc).limit(10).show
    //
    //          val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
    //            .drop(outDeg.col("id"))
    //            .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
    //
    //          degreeRatio.cache()
    //
    //          degreeRatio.orderBy($"degreeRatio".desc).limit(10).show()
    //          degreeRatio.orderBy($"degreeRatio".asc).limit(10).show()


    //val scc =graph.stronglyConnectedComponents.maxIter(5).run()
    //    scc.select("id", "component").orderBy($"component".desc).limit(10).show()
    //
    //    val lp = graph.labelPropagation.maxIter(5).run()
    //    lp.orderBy($"label".desc).limit(5).show()



    val triangles = getMinDistTriangles(graph)
      triangles.show()
    triangles.printSchema()

  }

  def getMinDistTriangles(graph: GraphFrame): Dataset[Row] = {
    val triangles = graph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
    triangles.select("*").where("a.id != b.id").where("b.id != c.id")
      .where("ab.date < bc.date").where("bc.date < ca.date")
    //      .orderBy("ab.distance + bc.distance + ca.distance")
    //      .orderBy(exp("ab.distance + bc.distance + ca.distance"))
  }

  def getStronglyConnectedComponents(graph: GraphFrame, maxIter: Int = 5, ascending: Boolean = false, sample: Boolean = true) = {
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
      val output = ratio.orderBy($"degreeRatio".asc)
      if (sample) output.limit(10).show()
      output
    }

    else {
      val output = ratio.orderBy($"degreeRatio".desc)
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
//      val dayOfMonth = flightRow.getAs[Int]("DayofMonth")
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

  def getAiports(graph: GraphFrame) ={
    graph.vertices.columns.toString
  }










  /**
    * Recibiendo dos arrays: Array[(String,String,Int)], edgeArray: Array[(String,String,Int)]
    * se construye un GraphFrame, partiendo de los Dataframe de vertices y edges convertidos
    *
    * @param vertexArray Array de vertices, cada vertice se define por una tupla (String,String,Int)
    * @param edgeArray Array de aristas, cada arista se define por una tupla (String,String,Int)
    * @return un grafo compuesto por los vertices y aristas que toma como parametro.
    */
  def getGraphFrame(vertexArray: Array[(String,String,Int)], edgeArray: Array[(String,String,Int)]):
  GraphFrame = {

    val vertex = sparkSession.createDataFrame(vertexArray).toDF("id", "name", "age")
    vertex.show(10,true)


    val edge = sparkSession.createDataFrame(edgeArray).toDF("src", "dst", "relationship")
    edge.show(10,true)

    val graphFrame: GraphFrame = GraphFrame(vertex, edge)

    graphFrame.cache
  }

  /**
    * Recibimos el grafo creado y vamos a quedarnos con los vertices
    * que sean estrictamente mayores de 'age' (argumento de la funcion)
    *
    * @param g Grafo a filtrar
    * @param age Edad para condicion de filtrado de los vertices
    */
  def showGreaterThan(g: GraphFrame, age: Int) = {
    g.vertices.filter($"age" > age).collect().foreach{
      case row => println(s"${row.getAs[Int]("name")} tiene ${row.getAs[Int]("age")} años")
    }
  }

  /**
    * Recibimos el grafo creado y vamos a quedarnos con los vertices
    * que cumplan una condicion de filtrado (argumento de la funcion)
    *
    * @param g Grafo a filtrar
    * @param fFilter funcion de filtrado de los vertices
    */
  def showFilteredVertex(g: GraphFrame, fFilter: Int => Boolean) = {
    g.vertices.filter(row => fFilter(row.getAs[Int]("age"))).collect().foreach{
      case row => println(s"${row.getAs[Int]("name")} tiene ${row.getAs[Int]("age")} años")
    }
  }

  /**
    * Recibimos el grafo creado y queremos mostrar los triplets del grafo (via join)
    *
    * @param g Grafo sobre el cual se muestran sus triplets (via join)
    */
  def showJoinTriplets(g: GraphFrame): Unit = {
    val tripletsOnlySrc: DataFrame = g.edges.join(g.vertices, $"src" === $"id")
      .select(
        $"src",
        $"dst",
        $"relationship",
        $"name".as("name_src"),
        $"age".as("age_src"))
    val triplets: DataFrame = tripletsOnlySrc.join(g.vertices, $"dst" === $"id")
      .select(
        $"src",
        $"dst",
        $"relationship",
        $"name_src",
        $"age_src",
        $"name".as("name_dst"),
        $"age".as("age_dst"))

    triplets.collect().foreach{
      case row =>
        println(s"${row.getAs[String]("name_src")} ha mandado ${row.getAs[Int]("relationship")} " +
          s"likes a ${row.getAs[String]("name_dst")}")
    }
  }

  /**
    * Recibimos el grafo creado y queremos mostrar los triplets del grafo (via DSL)
    *
    * @param g Grafo sobre el cual se muestran sus triplets (via join)
    */
  def showTripletsDSL(g: GraphFrame): Unit = {
    val triplets: DataFrame = g.find("(a)-[e]->(b)")
      .select($"a.name".as("src_name"), $"b.name".as("dst_name"), $"e.relationship".as("likes"))

    triplets.collect().foreach{
      case row =>
        println(s"${row.getAs[String]("src_name")} ha mandado ${row.getAs[Int]("likes")} " +
          s"likes a ${row.getAs[String]("dst_name")}")
    }
  }



  /**
    * Vamos a utilizar el metodo pageRank de la clase GraphFrame: g.pageRank.run()
    * En nuestro caso indicaremos dos parametros
    *   1. Maximo de iteraciones del algoritmo
    *   2. Probabilidad de reset o valor alpha, resetProb = 0.15
    *
    * Observacion: tras aplicar las variables hay que llamar explicitamente al metodo run()
    * Observacion 2: Nos quedaremos solo con el pageRank de los vertices, aunque tambien se computa el de las aristas
    *
    * @param g grafo sobre el que queremos mostrar su pageRank de vertices
    */
  def pageRankUserGraph(g: GraphFrame): Unit = {
    val rank: DataFrame = g.pageRank.maxIter(3).resetProbability(0.15).run().vertices
    // Y lo bueno es que no es necesario hacer join, el resultado es un Dataframe con una nueva columna pagerank
    rank.select($"pagerank",$"name").sort(-$"pagerank").show(10,true)
  }
}

