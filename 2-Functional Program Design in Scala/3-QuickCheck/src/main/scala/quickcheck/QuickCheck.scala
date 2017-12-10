package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val genHeap: Gen[H] = for {
    a <- arbitrary[A]
    h <- oneOf(genHeap,const(empty))
  }yield insert(a,h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)
  
   
  /**
   * General test:
   * - Take an arbitrary list of A
   * - Insert each element into a heap
   * - Recursively take the minimum of the heap and compare it to the sorted list
   */
  property("general test") = forAll { (l: List[A]) =>
    
    def list2heap(l: List[A]):H = if(l.isEmpty) empty else insert(l.head,list2heap(l.tail))
    
    def checkOrder(h:H,l:List[A]):Boolean = {
      if (l.isEmpty && isEmpty(h))
        //all elements have been matched in the right order
        //=> the implementation does the job
        true 
      else if (l.isEmpty || isEmpty(h))
        //the number of elements does not match.
        //=> implementation is bogus
        false 
      else
        l.head==findMin(h) && checkOrder(deleteMin(h), l.tail)
    }
    
    checkOrder(list2heap(l), l.sortBy(a => a))
  }
  
  
}
