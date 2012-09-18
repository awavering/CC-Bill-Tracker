package org.albertwavering.project;

public class CountingLong{
  long val = 1;

  public void CountingLong(){
    this.val = 1;
  }

  public long getVal(){
    return this.val;
  }

  public void incr(){
    this.val += 1;
  }
}
