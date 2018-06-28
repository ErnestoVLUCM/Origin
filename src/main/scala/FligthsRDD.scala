
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

object flightsRDD {
  private val src = "Origin"
  private val dst = "Dest"
  private val date_month = "Month"
  private val date_day_of_month = "DayofMonth"
  private val date_year = "Year"
  private val miles = "Distance"

  @transient lazy val sparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("Sparkflights")
      .config("spark.master", "local")
      .getOrCreate()

  def main(args:Array[String]): Unit = {
    sparkSession.sparkContext.setLogLevel("ERROR")


    val path =  "/home/evl/Escritorio/TFG/dataexpo_routes/2007.csv"
    val flightsDF = sparkSession.read.format("csv").option("path", path)
      .option("header", "true").option("inferSchema", "true").load.cache()



    println("Empezamos!")
    println("Las 5 rutas más repetidas son:")
    getTopNflights(flightsDF, 5).foreach(println(_))

    println("aeropuerto con mayor media de salidas por mes")
    println(getAiportWithMmaximusAvgDestByMonth(flightsDF).collect().foreach(println(_)))

    println("aeropuerto con menor media de salidas por mes")
    println(getAiportWithMinimusAvgSourceByMonth(flightsDF).collect().foreach(println(_)))

    println("aeropuerto con menor distancia por mes")
    println(getAiportWithMinimusDistanceByMonth(flightsDF).collect().foreach(println(_)))

    println("vuelos por mes")
    println(getFlightsByMonth(flightsDF).collect().foreach(println(_)))

    println("Ruta que mas veces se ha repetido 3 veces en la misma semana")
    println(getMaxNFlightsInAWeek(flightsDF,3))

    println("Ruta que menos veces se ha repetido 3 veces en la misma semana")
    println(getMinNFlightsInAWeek(flightsDF,3))
  }



  /**
    * Return the n routes more recurrent.
    */
  def getTopNflights(flightsDF: DataFrame, n: Int): Array[((String, String), Int)] = {
    val sqlContext = flightsDF.sqlContext
    import sqlContext.implicits._
    flightsDF.map(flightRow => {
      val origin = flightRow.getAs[String](src)
      val dest = flightRow.getAs[String](dst)
      ((origin, dest), 1)
    }).rdd.reduceByKey(_ + _).sortBy(_._2, ascending = false).take(n)
  }

  def getAiportWithMinimusAvgDestByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportWithMaximunOrMinimunAvgDestByMonth(flightsDF, maximun = false, dst).sortByKey()
  }
  def getAiportWithMmaximusAvgDestByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportWithMaximunOrMinimunAvgDestByMonth(flightsDF, sourceOrDest = dst).sortByKey()
  }


  def getAiportWithMinimusAvgSourceByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportWithMaximunOrMinimunAvgDestByMonth(flightsDF, maximun = false, src).sortByKey()
  }
  def getAiportWithMmaximusAvgSourceByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportWithMaximunOrMinimunAvgDestByMonth(flightsDF, sourceOrDest =  src).sortByKey()
  }

  def getAiportWithMaximunOrMinimunAvgDestByMonth(flightsDF: DataFrame, maximun: Boolean = true, sourceOrDest: String): RDD[(Int,(String,Double))] = {
    def reduceByKeyMin(rdd: RDD[(Int, (String,Double))]): RDD[(Int, (String,Double))] ={
      rdd.reduceByKey((current, next) =>
        if (current._2 < next._2) current
        else next)
    }
    def reduceByKeyMax(rdd: RDD[(Int, (String,Double))]): RDD[(Int, (String,Double))] ={
      rdd.reduceByKey((current, next) =>
        if (current._2 > next._2) current
        else next)
    }
    val sqlC = flightsDF.sqlContext
    import sqlC.implicits._

    val fMonthKeySuma = flightsDF.map(flightRow =>
      ((flightRow.getAs[Int](date_month),
        flightRow.getAs[String](sourceOrDest)), 1)).rdd.reduceByKey(_+_).cache()

    val fMonthSuma = fMonthKeySuma.map{case((mes, _), cont) =>
      (mes,cont)}.reduceByKey(_+_) //[month, count]

     val fMonthMesVuelos = fMonthKeySuma.map{case((mes, aeropuerto), cont) =>
      (mes, (aeropuerto, cont))} //[month, (aiportDest, count)]

    fMonthKeySuma.unpersist()
    val fmonthJoin = fMonthMesVuelos.join(fMonthSuma)

    val fmonthMedia = fmonthJoin.mapValues{case ((airport, airportCounter), monthCounter) =>
      (airport, (monthCounter/airportCounter).toDouble)}

    if (maximun) {
      reduceByKeyMax(fmonthMedia)
    } else{
      reduceByKeyMin(fmonthMedia)
    }
  }


  def getAiportWithMinimusDistanceByMonth(flightsDF: DataFrame): RDD[(Int,(String,Int))] = {

    //[(Mes,Aeropuerto),| Distancia] -> [(Mes, Aeropuerto),| SumatorioDistancias] -> [Mes,| (Aeropuerto, SumatorioDistancias] ->
    // -> cogemos el aeropuerto con menor distancia recorrida [Mes,| (MinAero, MinSumDis]
    val sqlContext = flightsDF.sqlContext
    import sqlContext.implicits._
    val fMonthKey = flightsDF.flatMap(flightRow => {
      val month = flightRow.getAs[Int](date_month)
      val origin = flightRow.getAs[String](src)
      val dest = flightRow.getAs[String](dst)
      val distance = flightRow.getAs[Int](miles)
      Seq(((month, dest), distance), ((month, origin), distance))})

    val monthKeySuma = fMonthKey.rdd.reduceByKey((cont1,cont2) => cont1+cont2)

    val fMonth = monthKeySuma.map{case((mes, aeropuerto), cont) => (mes,(aeropuerto, cont))}

    val fmin = fMonth.reduceByKey((airportDistancias1, airportDistancias2) => if (airportDistancias1._2 < airportDistancias2._2) airportDistancias1 else airportDistancias2)

    fmin.sortByKey()
  }

  def getFlightsByMonth(flightsDS: DataFrame): RDD[(Int, Int)] = {
    val sqlContext = flightsDS.sqlContext
    import sqlContext.implicits._
    flightsDS.map(flightRow => (flightRow.getAs[Int](date_month), 1)).rdd.reduceByKey(_+_).sortByKey()
  }


  def getMaxNFlightsInAWeek(flightsDS: DataFrame, n: Int): ((String, String), Int) = {
    getAiportNFlightsInAWeek(flightsDS, n)
  }
  def getMinNFlightsInAWeek(flightsDS: DataFrame, n: Int): ((String, String), Int) = {
    getAiportNFlightsInAWeek(flightsDS, n, max =false)
  }

  def getAiportNFlightsInAWeek(flightsDF: DataFrame, n: Int, max: Boolean = true): ((String, String), Int) = {
    def getMax(rDD: RDD[((String, String), Int)]): ((String, String), Int) = {
      rDD.sortByKey(ascending = false).first()
    }
    def getMin(rDD: RDD[((String, String), Int)]): ((String, String), Int) = {
      rDD.sortByKey().first()
    }

    val sqlContext = flightsDF.sqlContext
    import sqlContext.implicits._
    val weekOfYear = (year:Int, month:Int, day:Int) => new DateTime().year.setCopy(year).monthOfYear.setCopy(month)
      .dayOfMonth.setCopy(day).weekOfWeekyear.get

    val aiportsWithNFlights = flightsDF.map(flightRow => {
      val source = flightRow.getAs[String](src)
      val dest = flightRow.getAs[String](dst)
      val year = flightRow.getAs[Int](date_year)
      val month = flightRow.getAs[Int](date_month)
      val dayOfMonth = flightRow.getAs[Int](date_day_of_month)
      val date = weekOfYear(year, month, dayOfMonth)
      ((source, dest, date), 1)}).rdd.reduceByKey(_+_).filter(_._2 == n)

    println("in function")
    println(aiportsWithNFlights.take(6).foreach(println(_)))
    println("out")
    val countAiportsWithNFlights = aiportsWithNFlights.map{case ((source, dest, _), _) =>
      ((source, dest), 1)}.reduceByKey(_+_)

    if (max) getMax(countAiportsWithNFlights) else getMin(countAiportsWithNFlights)

  }


}
