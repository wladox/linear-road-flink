package com.github.wladox.model;

import lombok.Data;
import lombok.ToString;

/**
 * POJO holding input events.
 */
@Data
@ToString
public class Event {

  public Integer type;
  public Short time;
  public Integer vid;
  public Integer speed;
  public Integer xWay;
  public Integer lane;
  public Integer direction;
  public Integer segment;
  public Integer position;
  public String qid;
  public Integer day;
  public Integer minute;

  public Long ingestTime = -1L;
  public Boolean isCrossing = Boolean.FALSE;
  public Boolean isStopped = Boolean.FALSE;
  public Integer accInSegment = -1;
  public Integer samePositionCounter = 1;
  public Integer previousPosition = -1;
  public Integer nov = -1;
  public Integer lav = -1;
  public Integer toll = -1;

  public static Event parseFromString(String s) {
    String[] arr = s.replace("\n", "").split(",");
    Event e = new Event();
    e.setType(Integer.parseInt(arr[0]));
    e.setTime(Short.parseShort(arr[1]));
    e.setVid(Integer.parseInt(arr[2]));
    e.setSpeed(Integer.parseInt(arr[3]));
    e.setXWay(Integer.parseInt(arr[4]));
    e.setLane(Integer.parseInt(arr[5]));
    e.setDirection(Integer.parseInt(arr[6]));
    e.setSegment(Integer.parseInt(arr[7]));
    e.setPosition(Integer.parseInt(arr[8]));
    e.setQid(arr[9]);
    e.setDay(Integer.parseInt(arr[14]));
    e.setMinute(e.time / 60 + 1);
    return e;
  }



}
