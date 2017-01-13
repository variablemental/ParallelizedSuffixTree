import scala.collection.mutable.Map
/**
  * Created by coder-z on 17-1-12.
  */
class Node(parent:Node) {
  private var parentNode=parent
  private val edges=Map[Int,Edge]()
  def addChild(start:Int,end:Int):Node={
    val node=new Node(this)
    val newEdge=new Edge(start,end,node)
    edges+=(start->newEdge)
    node
  }
  def setParent(parent:Node)={
    this.parentNode=parent
  }
  def getEdge(start:Int)= edges(start)
  def getNode(start:Int)=edges(start).getDestination()
  def getParent()= parentNode
  def getEdges()=edges

}
