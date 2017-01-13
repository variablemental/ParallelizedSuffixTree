import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.tools.cmd.gen.AnyVals

/**
  * Created by coder-z on 17-1-12.
  */
object ParaelledSuffixTree extends Serializable{

  val L=10

  /**
    * 求最长公共字串长度
    * @param i  字符的位置
    * @param offset 偏移值
    * @param str  待处理的字符串
    * @return   最长的offset
    */
  def getCommonLen(i:Int,offset:Int,str:String): Int = {
    if(i-offset>=0&&i+offset<str.length) {
      if(str.substring(i-offset,i).equals(str.substring(i,i+offset)))
        getCommonLen(i,offset+1,str)
    }
    if(i+offset*2+1<str.length){
      if(str.substring(i,i+offset).equals(str.substring(i+offset+1,i+offset*2+1)))
        getCommonLen(i,offset+1,str)
    }
    offset-1
  }

  /**
    * 计算每个字符的最长公共子串长度
    * @param ch
    * @param str
    * @return
    */
  def calCommLen(ch:Char,str:String):Int={
    val index=str.indexOf(ch)
    var flag=true
    var max_len=if (L<str.length) L else str.length
    var len=max_len
    for(i<-0 to max_len if flag) {
      var offdex=str.indexOf(ch,index+1)
      while (offdex > 0&&offdex<str.length) {
        if(offdex+i>=str.length||(str.charAt(index+i)!=str.charAt(offdex+i))) {
          flag=false
          len=i
          offdex=0
        }
        else if(offdex+1<str.length)
          offdex=str.indexOf(ch,offdex+1)
      }
    }
    len
  }

  /**
    * 例如”BOOKEKEE“字符串
    *               K
    *               |
    *               E
    *              / \
    *             E   KEE
    * 是一个分支
    * 为每种字符创建后缀树分支
    * @param ch
    * @param common_len
    * @param str
    * @return
    */
  def createTree(ch:Char,common_len:Array[Int],str:String):SuffixTree={
    var pos=str.indexOf(ch)
    val root=new Node(null)
    val tree=new SuffixTree(root)
    var count=1
    while(pos!=(-1)) {
      for(i<-pos to str.length-1 by common_len(pos))
          tree.insert(i,i+common_len(pos))
      count+=1
      pos=find_next_char(ch,str,count)
    }
    tree
  }

  /**
    * 寻找字符串中下一个该字符的索引，找不打则返回-1
    * @param ch
    * @param str
    * @param count
    * @return
    */
  def find_next_char(ch:Char,str:String,count:Int): Int = {
    val index=str.indexOf(ch)
    var offdex=str.indexOf(ch,index+1)
    if(count==1)
      index
    else if(offdex+1>=str.length)
      -1
    for(i<-0 to count) {
      offdex=str.indexOf(ch,offdex+1)
    }
    offdex
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("paralleledSuffixTree")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("/home/coder-z/exset/ex0")
    val words = rawData.map { str =>
//      val charSequence = str.toCharArray
//      val common_rdd = sc.parallelize(charSequence)
//      val common_len = common_rdd.map { ch =>
//        calCommLen(ch, str)
//      }
//      common_len
      str.toCharArray
    }
    val common_len=words.zip(rawData).map {
      case (charS, str) => {
        val arrbuf=ArrayBuffer[Int]()
        charS.foreach(arrbuf+=calCommLen(_,str))
        (str,arrbuf.toArray)
      }
    }

    val forestRDD=words.zip(common_len).map{
      case(arr,pair) => {
        val forest=ArrayBuffer[SuffixTree]()
        arr.foreach(forest+=createTree(_,pair._2,pair._1))
        forest.toArray
      }
    }
    val record=forestRDD.map{pre =>
      pre.foreach(tree=>tree.printLeaf(tree.getRoot()))
      val arr=ArrayBuffer[String]()
      var str:String=null
      pre.foreach(tree=>(str+=tree.getResult()))
      str+""
      //arr.toArray
    }.saveAsTextFile("/home/coder-z/exset2")


//    val forestRDD = rawData.zip(words).map {
//      case (str, lens) =>
//        val charSequence = sc.parallelize(str.toCharArray)
//        charSequence.map(createTree(_, lens.toArray(), str))
//    }
//    val record = forestRDD.map(
//      _.map(tree => {
//        tree.printLeaf(tree.getRoot())
//        tree.getResult()
//      })
//    ).saveAsTextFile("/home/coder-z/exset")
  }
}
