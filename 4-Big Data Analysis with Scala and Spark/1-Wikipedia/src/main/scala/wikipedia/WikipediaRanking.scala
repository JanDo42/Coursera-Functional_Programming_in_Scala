package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("Wikipedia")
                        //.set("spark.executor.memory", "4g")
  
  val sc: SparkContext = new SparkContext(conf)
  val test = sc.textFile(WikipediaData.filePath)
  val wikiRdd: RDD[WikipediaArticle] =   sc.textFile(WikipediaData.filePath).map(WikipediaData.parse).persist()
                                                                        

  /** 
   *Returns the number of articles on which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.aggregate(0)((acc, article) => if(article.mentionsLanguage(lang)) acc+1 else acc, _+_)
  
 
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = 
    langs.map(l=>(l,occurrencesOfLang(l, rdd))) //map eacg language to a tuple (language, number of occurrences)
         .sortBy(x => -x._2)                    //sort by number of occurrences
 

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = 
    rdd.flatMap(article =>langs.filter(article.mentionsLanguage(_)) //Map each article to a list of languages it contains
       .map((_,article)))                                           //Map each contained language to a tuple (language,article)
       .groupByKey                                                  //Group by language

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = 
    index.map( entry => (entry._1,entry._2.size))  //Map each entry of the index to a tuple (language,number of articles containing it)
         .sortBy(-_._2)                            //Sort languages by occurrences
         .collect
         .toList

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = 
    rdd.flatMap(article =>langs.filter(article.mentionsLanguage(_))  //Map each article to a list of languages it contains
       .map((_,1)))                                                  //Map each contained language to a tuple (language,1)
       .reduceByKey(_ + _)                                           //Aggregate the 1's by language
       .sortBy(-_._2)                                                //Sort languages by occurrences
       .collect
       .toList
  

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
