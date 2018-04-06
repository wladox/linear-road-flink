package com.github.wladox.model;

import lombok.Data;

@Data
public class TollHistory {

  public Integer xWay;
  public Integer vid;
  public Integer day;
  public Integer toll;

  public TollHistory(Integer xWay, Integer vid, Integer day, Integer toll) {
    this.xWay = xWay;
    this.vid = vid;
    this.day = day;
    this.toll = toll;
  }
}
