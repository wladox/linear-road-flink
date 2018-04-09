package com.github.wladox.model;

import lombok.Data;

@Data
public class AccountBalance {

  public Integer balance;
  public Short lastUpdated;
  public Integer currentToll;

  public AccountBalance(Integer balance, Short lastUpdated, Integer currentToll) {
    this.balance = balance;
    this.lastUpdated = lastUpdated;
    this.currentToll = currentToll;
  }

  @Override
  public String toString() {
    return "AccountBalance{" +
      "balance=" + balance +
      ", lastUpdated=" + lastUpdated +
      ", currentToll=" + currentToll +
      '}';
  }
}
