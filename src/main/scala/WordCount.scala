import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {

  @transient lazy val sparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("WordCount")
      .config("spark.master", "local")
      .getOrCreate()

  def main(args:Array[String]): Unit = {
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val path = "~/data/Quijote.txt"
    val quijote: RDD[String] = sc.textFile(path)


    // lines count
    println(s"en el quijote hay: ${quijote.count()} lineas")


    // amount word
    val quijoteWordsCount = amountWordCount(quijote)
    println(s"en el quijote hay: $quijoteWordsCount palabras")

    // quijote count
    val word = "Dulcinea"
    val quijoteCount = wordCount(quijote, word)
    println(s"En el Quijote aparece $quijoteCount veces la palabra $word")

}

  /**
    * Return the number of word in the text.
    */
  def amountWordCount(rDD: RDD[String]): Long = {
    val quijoteWordsSplit = rDD.flatMap(line => line.split(" "))
    quijoteWordsSplit.count
  }
  /**
    * Returns the number of occurrences of the input word in the input RDD.
    */
  def wordCount(rDD: RDD[String], key: String): Long = {
    val words = rDD.flatMap(_.split(" "))
    words.map(_.toLowerCase().replaceAll("\\P{L1}","")).filter(_ == key.toLowerCase).count()
    words.map(_.toLowerCase().replaceAll("\\P{L}","")).filter(_ == key.toLowerCase).count()
  }
}