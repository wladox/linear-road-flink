package com.github.wladox.model;

import lombok.Data;

/**
 *
 */
@Data
public class Accident {

  public Integer firstVehicle;
  public Integer secondVehicle;
  public Integer created = 999999;
  public Integer cleared = -1;

  public Accident(Integer firstVehicle, Integer secondVehicle) {
    this.firstVehicle = firstVehicle;
    this.secondVehicle = secondVehicle;
  }
}
