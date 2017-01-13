/**
  * Created by coder-z on 17-1-12.
  *  树的结构：
  *         root
  *           | （start）
  *           | Edge
  *           |  （end）
  *          Node---------Node
  *        （start）Edge（end）
  *
  *
  */
class SuffixTree(rootNode:Node) {
  private val root=rootNode
  private var oldnode:Node=root
  private var result:String=null
  def insert(start:Int,end:Int): Node ={
    if(oldnode.getEdges().contains(start)&&oldnode.getEdge(start).getEnd()==end)
      oldnode.getEdge(start).getDestination()
    else
      oldnode=oldnode.addChild(start,end)
    oldnode
  }


  def printLeaf(node:Node): Unit = {
    if(node.getEdges().isEmpty) {
      node.getParent().getEdges().foreach(println(_))
      var tempNode=node
        while(tempNode.getParent()!=root)
          tempNode=tempNode.getParent()
      (root.getEdges().foreach(
        k=> if(k._2.getDestination()==tempNode) result+=k._2.getStart().toString else result+""
      ))
    }
    else {
      node.getEdges().foreach(
        key=>printLeaf(key._2.getDestination())
      )
    }
  }

  def getResult()=result
  def getRoot():Node=root
  def resetOldNode(): Unit = {oldnode=root}
}
