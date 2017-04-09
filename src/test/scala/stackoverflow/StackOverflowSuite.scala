package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  test("test stockoverflow") {
    import StackOverflow._
    val records = sc.parallelize(List("1,27233496,,,0,C#","1,23698767,,,9,C#","1,5484340,,,9,C#","2,5494879,,5484340,1,","1,9419744,,,2,Objective-C","1,26875732,,,1,C#","1,9002525,,,2,C++","2,9003401,,9002525,4,","2,9003942,,9002525,1,","2,9005311,,9002525,0,","1,5257894,,,1,Java","1,21984912,,,0,Java","2,21985273,,21984912,0,","1,27398936,,,0,PHP","1,28903923,,,0,PHP","2,28904080,,28903923,0,","1,20990204,,,6,PHP","1,5077978,,,-2,Python","2,5078493,,5077978,4,"))
    val raw     = rawPostings(records)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    assert(vectors.collect().size == 5, "invalid language group")
  }


}
