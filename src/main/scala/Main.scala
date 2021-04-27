package prediction
import com.sksamuel.elastic4s.fields.TextField
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import scala.util.control.Breaks._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.concurrent.duration._



object PredictionIndex extends App {

  val nodes = ElasticProperties("http://localhost:9200,http://localhost:9201")
  // in this example we create a client to a local Docker container at localhost:9200
  //val props = ElasticProperties("http://localhost:9200")
  val client = ElasticClient(JavaClient(nodes))

  // we must import the dsl
  import com.sksamuel.elastic4s.ElasticDsl._


  
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////  PART 1  /////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////



  //get current directory and add relative paths
  val dir = os.pwd / "data" / "tazi-se-interview-project-data.csv"

  /*

  //keep time counter

  def populateDataSourceSingle(dir: String, from: Int, until: Int): Int =  {
    val bufferedSource = io.Source.fromFile(dir)
    var instance: Int = 0

    for (line <- bufferedSource.getLines.slice(from, until)) {
      val cols = line.split(",").map(_.trim)

      client.execute {
        indexInto("prediction").id(cols(0)).fields(
          "given_label" -> cols(1),
          "model1_A"    -> cols(2).toDouble,
          "model1_B"    -> cols(3).toDouble,
          "model2_A"    -> cols(4).toDouble,
          "model2_B"    -> cols(5).toDouble,
          "model3_A"    -> cols(6).toDouble,
          "model3_B"    -> cols(7).toDouble
        ).refresh(RefreshPolicy.Immediate)
      }.await
      instance += 1
    }
    instance
  }

  def populateDataSourceMultiple(dir: String, from: Int, until: Int): Int = {
    val bufferedSource = io.Source.fromFile(dir)
    val arr = Array.ofDim[String](5, 8)
    var instance: Int = 0
    for (line <- bufferedSource.getLines.slice(from, until)) {
      val cols = line.split(",").map(_.trim)
      
      //Use bulk API for multiple indexing
      //Its much faster than one indexing at a time

      arr( (cols(0).toInt - 1)  % 5) = cols

      if( (cols(0).toInt) % 5 == 0 /*&& (cols(0).toInt - 1) != 0*/ ) {
        client.execute {
          bulk (
            indexInto("prediction").id(arr(0)(0)).fields(
              "given_label" -> arr(0)(1),
              "model1_A"    -> arr(0)(2).toDouble,
              "model1_B"    -> arr(0)(3).toDouble,
              "model2_A"    -> arr(0)(4).toDouble,
              "model2_B"    -> arr(0)(5).toDouble,
              "model3_A"    -> arr(0)(6).toDouble,
              "model3_B"    -> arr(0)(7).toDouble
            ),//.refresh(RefreshPolicy.Immediate)
            indexInto("prediction").id(arr(1)(0)).fields(
              "given_label" -> arr(1)(1),
              "model1_A"    -> arr(1)(2).toDouble,
              "model1_B"    -> arr(1)(3).toDouble,
              "model2_A"    -> arr(1)(4).toDouble,
              "model2_B"    -> arr(1)(5).toDouble,
              "model3_A"    -> arr(1)(6).toDouble,
              "model3_B"    -> arr(1)(7).toDouble
            ),//.refresh(RefreshPolicy.Immediate)
            indexInto("prediction").id(arr(2)(0)).fields(
              "given_label" -> arr(2)(1),
              "model1_A"    -> arr(2)(2).toDouble,
              "model1_B"    -> arr(2)(3).toDouble,
              "model2_A"    -> arr(2)(4).toDouble,
              "model2_B"    -> arr(2)(5).toDouble,
              "model3_A"    -> arr(2)(6).toDouble,
             "model3_B"    -> arr(2)(7).toDouble
            ),//.refresh(RefreshPolicy.Immediate)
            indexInto("prediction").id(arr(3)(0)).fields(
              "given_label" -> arr(3)(1),
              "model1_A"    -> arr(3)(2).toDouble,
              "model1_B"    -> arr(3)(3).toDouble,
              "model2_A"    -> arr(3)(4).toDouble,
              "model2_B"    -> arr(3)(5).toDouble,
              "model3_A"    -> arr(3)(6).toDouble,
              "model3_B"    -> arr(3)(7).toDouble
            ),//.refresh(RefreshPolicy.Immediate)
            indexInto("prediction").id(arr(4)(0)).fields(
              "given_label" -> arr(4)(1),
              "model1_A"    -> arr(4)(2).toDouble,
              "model1_B"    -> arr(4)(3).toDouble,
              "model2_A"    -> arr(4)(4).toDouble,
              "model2_B"    -> arr(4)(5).toDouble,
              "model3_A"    -> arr(4)(6).toDouble,
              "model3_B"    -> arr(4)(7).toDouble
            )//.refresh(RefreshPolicy.Immediate)
          )
        }.await
      }

      instance += 1
      println("id: " + cols(0))
    }
    instance
  }

  //start timer

  val startTime = System.currentTimeMillis()


  val thread1Future: Future[Int] = Future {
    val instanceNumber: Int = populateDataSourceSingle(dir.toString, 1, 71)
    instanceNumber
  }

  val thread2Future: Future[Int] = Future {
    val instanceNumber: Int = populateDataSourceSingle(dir.toString, 71, 151)
    instanceNumber
  }

  val thread3Future: Future[Int] = Future {
    val instanceNumber: Int = populateDataSourceSingle(dir.toString, 151, 201)
    instanceNumber
  }

  
  val result: Future[(Int, Int,Int)] = for {
    result1    <- thread1Future
    result2    <- thread2Future
    result3    <- thread3Future
    //result4  <- thread4Future
    //result5  <- thread5Future
  } yield (result1, result2, result3)
  
  
  //save the return variable when result is available
  //Force program to block
  val threadResults = Await.result(result, Duration.Inf)

  //Calculate total time in minutes for poplulating data source
  val deltaTime = (System.currentTimeMillis() - startTime) / (1000.0  ) // in seconds

  println("\n\n=> Total time for 200 instance is " + deltaTime + " second")

  //println("Total instance: " + threadResults )
  //println("Instance per second: " + threadResults / deltaTime )

  println("=> Total instance: " + (threadResults._1 + threadResults._2 + threadResults._3) )
  println("=> Instance per second: " + (threadResults._1 + threadResults._2 + threadResults._3) / deltaTime + "\n" )

  */
  
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////   PART 2  ////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  

  
  def calculateProb(model1: Double, model2: Double, model3:Double): Double = {
    //weights for models
    val weight1 = 0.5
    val weight2 = 0.6
    val weight3 = 0.7

    return (model1*weight1 + model2*weight2 + model3*weight3) / (weight1 + weight2 + weight3)
  }

  def findConfusion(predictions_A: Array[Double], predictions_B: Array[Double], given_label: String): String = {
    /*
    get predictions for A and B label,
    returns confusion(TP, FN, FP or TN)
    */
    var weightedAverage_A: Double = calculateProb(predictions_A(0), predictions_A(1), predictions_A(2))
    var weightedAverage_B: Double = calculateProb(predictions_B(0), predictions_B(1), predictions_B(2))
    //compare them and get predicted label
    var predicted_label: String = ""
    if (weightedAverage_A > weightedAverage_B) { predicted_label = "A" } else { predicted_label = "B" }
    //conclusion
    var confusion: String = ""
    if (given_label == "A" && predicted_label == "A")      { confusion = "TP" }
    else if (given_label == "A" && predicted_label == "B") { confusion = "FN" }
    else if (given_label == "B" && predicted_label == "A") { confusion = "FP" }
    if (given_label == "B" && predicted_label == "B")      { confusion = "TN" }

    return confusion

  }

  def indexWindows(dir: String, start: Int, finish: Int): Int = {
    //hash map for confusion matrix
    import scala.collection.mutable.HashMap
    val confusionMatrix: HashMap[String, Int] = HashMap("TP"->0,"FN"->0,"FP"->0,"TN"->0)

    //queue for sliding window
    import scala.collection.mutable.Queue
    val slidingWindow = Queue[String]()

    //Process CSV file line by line
    val bufferedSource = io.Source.fromFile(dir)
    var window: Int                = 1
    var after1000instance: Boolean = false
    var headOfWindow:String        = ""
    var confusion: String          = ""

    val temp = Array.ofDim[Int](5, 5)

    for (line <- bufferedSource.getLines.slice(start, finish)) {
        val cols = line.split(",").map(_.trim)

        if (after1000instance) {

          //remove head of window
          //together with its confusion
          headOfWindow = slidingWindow.dequeue
          confusionMatrix(headOfWindow) -= 1
          //calculate new instance
          //and add to queue
          
          confusion = findConfusion(
            predictions_A = Array(cols(2).toDouble, cols(4).toDouble, cols(6).toDouble), 
            predictions_B =  Array(cols(3).toDouble, cols(5).toDouble, cols(7).toDouble),
            given_label = cols(1)
          )

          confusionMatrix(confusion) += 1

          //add confusion to the queue
          slidingWindow.enqueue(confusion)

          temp( (cols(0).toInt - 1) % 5) = Array(
            cols(0).toInt - 999,
            confusionMatrix("TP"),
            confusionMatrix("FN"),
            confusionMatrix("FP"),
            confusionMatrix("TN")
          )

          //lastly index the confusion matrix
          /*
          client.execute {
            indexInto("confusion").id(window.toString).fields(
              "True Positive"  -> confusionMatrix("TP"),
              "False Negative" -> confusionMatrix("FN"),
              "False Positive" -> confusionMatrix("FP"),
              "True Negative"  -> confusionMatrix("TN")
            ).refresh(RefreshPolicy.Immediate)
          }.await
          */

          //////////////////////////////////
          window += 1
          println("SLIDING WINDOW: " + window)

          if( (cols(0).toInt) % 5 == 0 /*&& (cols(0).toInt - 1) != start + 1000*/ ) {
            client.execute {
              bulk (
                indexInto("confusion").id(temp(0)(0).toString).fields(
                  "True Positive"  -> temp(0)(1).toDouble,
                  "False Negative" -> temp(0)(2).toDouble,
                  "False Positive" -> temp(0)(3).toDouble,
                  "True Negative"  -> temp(0)(4).toDouble
                ),
                indexInto("confusion").id(temp(1)(0).toString).fields(
                  "True Positive"  -> temp(1)(1).toDouble,
                  "False Negative" -> temp(1)(2).toDouble,
                  "False Positive" -> temp(1)(3).toDouble,
                  "True Negative"  -> temp(1)(4).toDouble
                ),
                indexInto("confusion").id(temp(2)(0).toString).fields(
                  "True Positive"  -> temp(2)(1).toDouble,
                  "False Negative" -> temp(2)(2).toDouble,
                  "False Positive" -> temp(2)(3).toDouble,
                  "True Negative"  -> temp(2)(4).toDouble
                ),
                indexInto("confusion").id(temp(3)(0).toString).fields(
                  "True Positive"  -> temp(3)(1).toDouble,
                  "False Negative" -> temp(3)(2).toDouble,
                  "False Positive" -> temp(3)(3).toDouble,
                  "True Negative"  -> temp(3)(4).toDouble
                ),
                indexInto("confusion").id(temp(4)(0).toString).fields(
                  "True Positive"  -> temp(4)(1).toDouble,
                  "False Negative" -> temp(4)(2).toDouble,
                  "False Positive" -> temp(4)(3).toDouble,
                  "True Negative"  -> temp(4)(4).toDouble
                )
              )
            }.await
          }
        }
        
        else {
          confusion = findConfusion(
            predictions_A = Array(cols(2).toDouble, cols(4).toDouble, cols(6).toDouble), 
            predictions_B =  Array(cols(3).toDouble, cols(5).toDouble, cols(7).toDouble),
            given_label = cols(1)
          )

          confusionMatrix(confusion) += 1

          //add confusion to the queue
          slidingWindow.enqueue(confusion)          
        }

        //if sliding windows reaches size of 1000

        if (cols(0).toInt == start + 1000 - 1) {
          //First window
          
          //Index confusion matrix to ElasticSearch

          client.execute {
            indexInto("confusion").id( (cols(0).toInt - 999).toString ).fields(
              "True Positive"  -> confusionMatrix("TP"),
              "False Negative" -> confusionMatrix("FN"),
              "False Positive" -> confusionMatrix("FP"),
              "True Negative"  -> confusionMatrix("TN")
            ).refresh(RefreshPolicy.Immediate)
          }.await

          window += 1
          after1000instance = true
        }
    }
    bufferedSource.close
    return window - 1
  }

  val startTime = System.currentTimeMillis()

  

  val thread1: Future[Int] = Future {
    val thread1StartTime = System.currentTimeMillis()
    val windowNumber: Int = indexWindows(dir.toString, 1, 2001) //5.001 not included
    val deltaTime = (System.currentTimeMillis() - thread1StartTime) / 1000.0
    println("Thread 1 time: " + deltaTime)
    windowNumber
  }
  val thread2: Future[Int] = Future {
    val thread2StartTime = System.currentTimeMillis()
    val windowNumber: Int = indexWindows(dir.toString, 1001, 3001) //10.001 not included
    val deltaTime = (System.currentTimeMillis() - thread2StartTime) / 1000.0
    println("Thread 2 time: " + deltaTime)
    windowNumber
  }
  val thread3: Future[Int] = Future {
    val thread3StartTime = System.currentTimeMillis()
    val windowNumber: Int = indexWindows(dir.toString, 2001, 4001) //10.001 not included
    val deltaTime = (System.currentTimeMillis() - thread3StartTime) / 1000.0
    println("Thread 3 time: " + deltaTime)
    windowNumber
  }

  val result: Future[(Int, Int, Int)] = for {
    window1    <- thread1
    window2    <- thread2
    window3    <- thread3
    //result4  <- thread4Future
    //result5  <- thread5Future
  } yield (window1, window2, window3)

  
  val windowNumbers = Await.result(result, Duration.Inf)

  //data population is completed
  val difference = (System.currentTimeMillis() - startTime) / (1000.0) //seconds


  println("\n\n=> Total time for part ii : " + difference + " second")
  println("=> Number of confusion matrix : " + (windowNumbers._1 + windowNumbers._2 + windowNumbers._3) )
  println("=> Confusion matrix per second: " + (windowNumbers._1 + windowNumbers._2 + windowNumbers._3) / difference + "\n")

  
  

  client.close()  
  java.awt.Toolkit.getDefaultToolkit.beep()
}