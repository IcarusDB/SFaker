package org.cheney.fake.generator

object SeqToArray {
  def copy(seq: Seq[Object]): Array[Object] = {
    seq.toArray[Object]
  }
}
