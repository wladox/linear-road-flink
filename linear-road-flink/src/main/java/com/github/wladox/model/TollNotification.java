package com.github.wladox.model;

import lombok.Data;

/**
 * Created by root on 05.04.18.
 */

@Data
public class TollNotification {

  public String carId;
  public String time;
  public String emit;
  public String lav;
  public String toll;

  public TollNotification(String carId, String time, String emit, String lav, String toll) {
    this.carId = carId;
    this.time = time;
    this.emit = emit;
    this.lav = lav;
    this.toll = toll;
  }

  @Override
  public String toString() {
    return String.format("%d,%s,%s,%s,%s,%s", 0, carId, time, emit, lav, toll);
  }
}
