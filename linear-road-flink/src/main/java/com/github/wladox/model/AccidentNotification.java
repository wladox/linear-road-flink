package com.github.wladox.model;

import lombok.Data;

/**
 * Created by root on 05.04.18.
 */
@Data
public class AccidentNotification {

  public String time;
  public String emit;
  public String xWay;
  public String segment;
  public String direction;
  public String carId;

  @Override
  public String toString() {
    return String.format("1,%s,%s,%s,%s,%s,%s\n", time, emit, xWay, segment, direction, carId);
  }
}
