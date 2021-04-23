import com.sksamuel.elastic4s.fields.TextField
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import scala.util.control.Breaks._


object PredictionIndex extends App {

  // in this example we create a client to a local Docker container at localhost:9200
  //val props = ElasticProperties("http://localhost:9200")
  //val client = ElasticClient(JavaClient(props))

  // we must import the dsl
  import com.sksamuel.elastic4s.ElasticDsl._
  
  //////////////////////////////////////////////PART 1/////////////////////////////////////////////////////


  //get current directory and add relative paths
  val dir = os.pwd / "data" / "tazi-se-interview-project-data.csv"

  //keep time counter
  val start = System.nanoTime

  

  //read and process csv file
  val bufferedSource = io.Source.fromFile(dir.toString)
    for (line <- bufferedSource.getLines.drop(1)) {
        val cols = line.split(",").map(_.trim)
        //index the document
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
        //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}|${cols(4)}|${cols(5)}|${cols(6)}|${cols(7)}")
        println("id: " + cols(0))
    }
    bufferedSource.close

  

  //////////////////////////////// PART 2    ////////////////////////////////////////////////////////////

  def calculateProb(model1: Double, model2: Double, model3:Double): Double = {
    //weights for models
    val weight1 = 0.5
    val weight2 = 0.6
    val weight3 = 0.7

    return (model1*weight1 + model2*weight2 + model3*weight3) / 3
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

  val props = ElasticProperties("http://localhost:9200")
  val client = ElasticClient(JavaClient(props))

  /*
  client.execute {
    createIndex("confusion_matrix")mappings(
      properties(
        IntegerField("True Positive"),
        IntegerField("False Negative"),
        IntegerField("False Positive"),
        IntegerField("True Negative")

        //"True Positive" typed IntegerType,
        //"False Negative" typed IntegerType,
        //"False Positive" typed IntegerType,
        //"True Negative" typed IntegerType
      )
    )
  }.await
  */


  //hash map for confusion matrix
  import scala.collection.mutable.HashMap

  val confusionMatrix: HashMap[String, Int] = HashMap("TP"->0,"FN"->0,"FP"->0,"TN"->0)

  //queue for sliding window
  import scala.collection.mutable.Queue

  val slidingWindow = Queue[String]()

  val bufferedSource = io.Source.fromFile(dir.toString)
    var window: Int                = 1
    var after1000instance: Boolean = false
    var headOfWindow:String        = ""
    var confusion: String          = ""

    for (line <- bufferedSource.getLines.drop(1)) {
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

          //lastly index the confusion matrix
          client.execute {
            indexInto("confusion_matrix").id(window.toString).fields(
              "True Positive"  -> confusionMatrix("TP"),
              "False Negative" -> confusionMatrix("FN"),
              "False Positive" -> confusionMatrix("FP"),
              "True Negative"  -> confusionMatrix("TN")
            ).refresh(RefreshPolicy.Immediate)
          }.await

          //////////////////////////////////
          window += 1
          println("SLIDING WINDOW: " + window)
        }
        

        confusion = findConfusion(
          predictions_A = Array(cols(2).toDouble, cols(4).toDouble, cols(6).toDouble), 
          predictions_B =  Array(cols(3).toDouble, cols(5).toDouble, cols(7).toDouble),
          given_label = cols(1)
        )

        confusionMatrix(confusion) += 1

        //add confusion to the queue
        slidingWindow.enqueue(confusion)

        //if sliding windows reaches size of 1000

        if (cols(0).toInt == 1000) {
          //First window
          
          //Index confusion matrix to ElasticSearch

          client.execute {
            indexInto("confusion_matrix").id(window.toString).fields(
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

  //data population is completed
  val difference = (System.nanoTime - start) / 1e9d

  println("Time: " + difference + " second")
  

  client.close()  
}