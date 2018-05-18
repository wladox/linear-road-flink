package com.github.wladox.model;

import lombok.Data;

@Data
public class AccountBalanceResponse {

  public Short time;
  public Long emit;
  public String qid;
  public Short resultTime;
  public Integer currentBalance;

  public AccountBalanceResponse(Short time, Long emit, Short resultTime, String qid, Integer currentBalance) {
    this.time = time;
    this.emit = emit;
    this.qid = qid;
    this.resultTime = resultTime;
    this.currentBalance = currentBalance;
  }

  @Override
  public String toString() {
    return String.format("2,%d,%d,%d,%s,%d", time, emit, resultTime, qid, currentBalance);
  }
}
