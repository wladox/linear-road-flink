package com.github.wladox.model;

import lombok.Data;

/**
 *
 */
@Data
public class Accident {

  public Integer firstVehicle;
  public Integer secondVehicle;
  public Short created;
  public Integer cleared = -1;

  public Accident(Integer firstVehicle, Integer secondVehicle, Short created) {
    this.firstVehicle = firstVehicle;
    this.secondVehicle = secondVehicle;
    this.created = created;
  }
}
