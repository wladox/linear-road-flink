package com.github.wladox.model;

import lombok.Data;

/**
 * Created by root on 05.04.18.
 */
@Data
public class AccidentNotification {

  public Short time;
  public Long emit;
  public Integer xWay;
  public Integer segment;
  public Integer direction;
  public Integer carId;

  public AccidentNotification(Short time, Long emit, Integer xWay, Integer segment, Integer direction, Integer carId) {
    this.time = time;
    this.emit = emit;
    this.xWay = xWay;
    this.segment = segment;
    this.direction = direction;
    this.carId = carId;
  }

  @Override
  public String toString() {
    return String.format("1,%d,%d,%d,%d,%d,%d", time, emit, xWay, segment, direction, carId);
  }
}
