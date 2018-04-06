package com.github.wladox.model;

import lombok.Data;

@Data
public class AccountBalance {

  public Integer previousToll;
  public Integer currentTime;
  public Integer currentToll;

  @Override
  public String toString() {
    return "AccountBalance{" +
      "previousToll=" + previousToll +
      ", currentTime=" + currentTime +
      ", currentToll=" + currentToll +
      '}';
  }
}
