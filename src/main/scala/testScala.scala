object testScala {
  def main(args: Array[String]): Unit = {
    var iterators: Iterator[(String,String)]=Iterator(("tony","19"),("jack","20"))
    // 发现执行了size操作terators.hasNext一定为false，坑爹的迭代器
    println("执行之前size："+iterators.size)
    println("执行之前hasNext："+iterators.hasNext)
    while(iterators.hasNext){
      println(iterators.next())
    }
    println("执行之后size："+iterators.size)
    println("执行之后hasNext："+iterators.hasNext)

  }
}
