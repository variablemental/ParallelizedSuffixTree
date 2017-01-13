import org.apache.hadoop.hdfs.web.resources.DestinationParam

/**
  * Created by coder-z on 17-1-12.
  */
class Edge(startParam:Int,endParam:Int,destinationParam:Node) {
  private val start=startParam
  private val end=endParam
  private val destination=destinationParam
  def this(start:Int)=this(start,Integer.MAX_VALUE,null)
  def getDestination()=destination
  def getStart():Int=start
  def getEnd():Int=end
}
