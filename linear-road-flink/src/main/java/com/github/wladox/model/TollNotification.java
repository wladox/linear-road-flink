package com.github.wladox.model;

import lombok.Data;

/**
 * Created by root on 05.04.18.
 */

@Data
public class TollNotification {

  public Integer carId;
  public Short time;
  public Long emit;
  public Integer lav;
  public Integer toll;


  public TollNotification(Integer carId, Short time, Long emit, Integer lav, Integer toll) {
    this.carId = carId;
    this.time = time;
    this.emit = emit;
    this.lav = lav;
    this.toll = toll;
  }

  @Override
  public String toString() {
    return String.format("%d,%s,%s,%d,%s,%s", 0, carId, time, emit, lav, toll);
  }
}
