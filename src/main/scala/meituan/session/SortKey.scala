package meituan.session

/**
  * @ author: create by LuJuHui
  * @ date:2018/8/13
  */
class SortKey(val clickCount:Int,val orderCount:Int,val payCouont:Int)
  extends Ordered[SortKey] with Serializable{
  override def compare(that: SortKey): Int = {
    if((clickCount - that.clickCount) != 0){
      clickCount - that.clickCount
    }else if((orderCount - that.orderCount) != 0){
      orderCount - that.orderCount
    }else if((payCouont -that.payCouont) != 0){
      payCouont -that.payCouont
    }else{
      0
    }
  }
}

