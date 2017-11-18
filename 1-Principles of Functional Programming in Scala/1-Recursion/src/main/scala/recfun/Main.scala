package recfun

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
   
  }

  /**
   * Exercise 1
   */
    def pascal(c: Int, r: Int): Int = {
     
      if( c==0 || c==r ) 
        1
      else
        pascal(c,r-1)+pascal(c-1,r-1)
    }
  
  /**
   * Exercise 2
   */
    def balance(chars: List[Char]): Boolean = {
      def balance(count: Int, chars: List[Char]):Boolean ={
   
        if(count == -1)
          false
        else if(chars.isEmpty)
          count == 0
        else if(chars.head=='(')
          balance(count+1,chars.tail)
        else if(chars.head==')')
          balance(count-1,chars.tail)
        else
          balance(count,chars.tail)
          
        
      }
      balance(0,chars)
    }
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = {
      if(money<=0 || coins.isEmpty)
        0
      else if(money==coins.head)
        1+countChange(money,coins.tail)
      else if(coins.head<0)
        countChange(money,coins.tail)
      else
        countChange(money-coins.head,coins)+countChange(money,coins.tail)
    
    }
  }
