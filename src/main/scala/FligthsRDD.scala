
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

object flightsRDD {
  private val origin = "Origin"
  private val dest = "Dest"
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


    val path =  "~/data/2001.csv"
    val flightsDF = sparkSession.read.format("csv").option("path", path)
      .option("header", "true").option("inferSchema", "true").load.cache()

    println("Empezamos!")

    println("Las 5 rutas más repetidas son:")
    getTopNflights(flightsDF, 5).foreach(println(_))

    println("aeropuerto con mayor media de salidas por mes:")
    getAiportsWithMaximusAvgSourceByMonth(flightsDF).collect().foreach(println(_))

    println("Aeropuerto con menor media de salidas por mes:")
    getAiportsWithMinimusAvgSourceByMonth(flightsDF).collect().foreach(println(_))

    println("Aeropuerto con menor distancia por mes:")
    getAiportWithMinimusDistanceByMonth(flightsDF).collect().foreach(println(_))

    println("vuelos por mes:")
    getFlightsByMonth(flightsDF).collect().foreach(println(_))

    println("Cantidad mensual de vuelos por aeropuerto:")
    getMonthlyProgression(flightsDF).take(5).foreach(println(_))

    println("Ruta que mas veces se ha repetido 3 veces en la misma semana:")
    println(getMaxNFlightsInAWeek(flightsDF,3))

    println("Ruta que menos veces se ha repetido 3 veces en la misma semana:")
    println(getMinNFlightsInAWeek(flightsDF,2))
  }


  /**
    * Return the n routes more recurrent.
    */
  def getTopNflights(flightsDF: DataFrame, n: Int): Array[((String, String), Int)] = {
    val sqlContext = flightsDF.sqlContext
    import sqlContext.implicits._
    flightsDF.map(flightRow => {
      val source = flightRow.getAs[String](origin)
      val destination = flightRow.getAs[String](dest)
      ((source, destination), 1)
    }).rdd.reduceByKey(_ + _).sortBy(_._2, ascending = false).take(n)
  }

  /**
    * Returns for each month the airport with the lowest average of flights that have landed on it.
    */
  def getAiportsWithMinimusAvgDestByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportsWithMaximunOrMinimunAvgDestOrSourceByMonth(flightsDF, maximun = false, dest).sortByKey()
  }

  /**
  Returns for each month the airport with the lowest average of flights that have taken off from it.
    */
  def getAiportsWithMaximusAvgDestByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportsWithMaximunOrMinimunAvgDestOrSourceByMonth(flightsDF, sourceOrDest = dest).sortByKey()
  }


  /**
  Returns for each month the airport with the highest average of flights that have landed on it.
   */
  def getAiportsWithMinimusAvgSourceByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportsWithMaximunOrMinimunAvgDestOrSourceByMonth(flightsDF, maximun = false, origin).sortByKey()
  }

  /**
  Returns for each month the airport with the highest average of flights that have taken off from it.
    */
  def getAiportsWithMaximusAvgSourceByMonth(flightsDF: DataFrame): RDD[(Int,(String,Double))] = {
    getAiportsWithMaximunOrMinimunAvgDestOrSourceByMonth(flightsDF, sourceOrDest =  origin).sortByKey()
  }

  def getAiportsWithMaximunOrMinimunAvgDestOrSourceByMonth
    (flightsDF: DataFrame, maximun: Boolean = true, sourceOrDest: String): RDD[(Int,(String,Double))] = {

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
      ((flightRow.getAs[Int](date_month), flightRow.getAs[String](sourceOrDest)),
        1)).rdd.reduceByKey(_+_).cache()

    val fMonthSuma = fMonthKeySuma.map{case((month, _), count) =>
      (month,count)}.reduceByKey(_+_)

     val fMonthFligths = fMonthKeySuma.map{case((month, airport), count) =>
      (month, (airport, count))}

    fMonthKeySuma.unpersist()
    val fmonthJoin = fMonthFligths.join(fMonthSuma)

    val fmonthMedia = fmonthJoin.mapValues{case ((airport, airportCounter), monthCounter) =>
      (airport, (monthCounter/airportCounter).toDouble)}

    if (maximun) {
      reduceByKeyMax(fmonthMedia)
    } else{
      reduceByKeyMin(fmonthMedia)
    }
  }

  /**
  Returns for each month the airport with the lowest distance traveled by
  the flights that have landed and taken off from it.
    */
  def getAiportWithMinimusDistanceByMonth(flightsDF: DataFrame): RDD[(Int,(String,Int))] = {


    val sqlContext = flightsDF.sqlContext
    import sqlContext.implicits._
    val fMonthKey = flightsDF.flatMap(flightRow => {
      val month = flightRow.getAs[Int](date_month)
      val source = flightRow.getAs[String](origin)
      val destination = flightRow.getAs[String](dest)
      val distance = flightRow.getAs[Int](miles)
      Seq(((month, destination), distance), ((month, source), distance))})

    val monthKeySum = fMonthKey.rdd.reduceByKey((count1,count2) => count1+count2)

    val fMonth = monthKeySum.map{case((month, airport), count) => (month,(airport, count))}

    val fmin = fMonth.reduceByKey((current, next) => if (current._2 < next._2) current else next)

    fmin.sortByKey()
  }

  /**
  Returns the number of flights that have landed and taken off per month at each airport.
    */
  def getMonthlyProgression[T](flightsDF: DataFrame): RDD[(String, List[(Int, Int)])] = {
    val sqlContext = flightsDF.sqlContext
    import sqlContext.implicits._

    val fMonthKey = flightsDF.flatMap(flightRow => {
      val source = flightRow.getAs[String](origin)
      val destination = flightRow.getAs[String](dest)
      val month = flightRow.getAs[Int](date_month)
      Seq(((destination, month), 1), ((source, month), 1))})

    val fAiportMonthKeySum = fMonthKey.rdd.reduceByKey(_+_)
    fAiportMonthKeySum.map{case((aeropuerto, mes), cont) => (aeropuerto, (mes, cont))}
      .groupByKey().mapValues(_.toList.sortBy(_._1))
  }

  /**
  Returns the number of flights for each month.
    */
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
      val source = flightRow.getAs[String](origin)
      val destination = flightRow.getAs[String](dest)
      val year = flightRow.getAs[Int](date_year)
      val month = flightRow.getAs[Int](date_month)
      val dayOfMonth = flightRow.getAs[Int](date_day_of_month)
      val date = weekOfYear(year, month, dayOfMonth)
      ((source, destination, date), 1)}).rdd.reduceByKey(_+_).filter(_._2 == n)

    val countAiportsWithNFlights = aiportsWithNFlights.map{case ((source, dest, _), _) =>
      ((source, dest), 1)}.reduceByKey(_+_)

    if (max) getMax(countAiportsWithNFlights) else getMin(countAiportsWithNFlights)
  }


}
