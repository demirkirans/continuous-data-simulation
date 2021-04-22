import com.sksamuel.elastic4s.fields.TextField
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.searches.SearchResponse



object PredictionIndex extends App {

  // in this example we create a client to a local Docker container at localhost:9200

  //get current directory and add relative paths
  val dir = os.pwd / "csv" / "tazi-se-interview-project-data.csv"

  //read and process csv file
  val bufferedSource = io.Source.fromFile(dir.toString)
    for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        // do whatever you want with the columns here
        println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}|${cols(4)}|${cols(5)}|${cols(6)}|${cols(7)}")
    }
    bufferedSource.close



  

  val props = ElasticProperties("http://localhost:9200")
  val client = ElasticClient(JavaClient(props))

  // we must import the dsl
  import com.sksamuel.elastic4s.ElasticDsl._

  // Next we create an index in advance ready to receive documents.
  // await is a helper method to make this operation synchronous instead of async
  // You would normally avoid doing this in a real program as it will block
  // the calling thread but is useful when testing
  
  
  client.execute {
    createIndex("artists").mapping(
      properties(
        TextField("name")
      )
    )
  }.await
  
  val start = System.nanoTime
  // Next we index a single document which is just the name of an Artist.
  // The RefreshPolicy.Immediate means that we want this document to flush to the disk immediately.
  // see the section on Eventual Consistency.
  client.execute {
    indexInto("customer").fields("name" -> "Muso", "surname" -> "Demirkiran").refresh(RefreshPolicy.Immediate)
  }.await

  

  // now we can search for the document we just indexed

  

  val resp = client.execute {
    search("artists").query("lowry")
  }.await

  

  // resp is a Response[+U] ADT consisting of either a RequestFailure containing the
  // Elasticsearch error details, or a RequestSuccess[U] that depends on the type of request.
  // In this case it is a RequestSuccess[SearchResponse]

  

  println("---- Search Results ----")
  resp match {
    case failure: RequestFailure => println("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] => println(results.result.hits.hits.toList)
    case results: RequestSuccess[_] => println(results.result)
  }

  

  // Response also supports familiar combinators like map / flatMap / foreach:
  //resp foreach (search => println(s"There were ${search.totalHits} total hits"))

  val finish = (System.nanoTime - start) / 1e9d

  client.close()

  
  
}