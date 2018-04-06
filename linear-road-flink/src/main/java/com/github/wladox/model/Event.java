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
  public Byte xWay;
  public Byte lane;
  public Byte direction;
  public Byte segment;
  public Integer position;
  public String qid;
  public Integer day;
  public Integer minute;

  public boolean isCrossing;
  public boolean isStopped;
  public Integer accInSegment;
  public int samePositionCounter;
  public int previousPosition;

  public static Event parseFromString(String s) {
    String[] arr = s.replace("\n", "").split(",");
    Event e = new Event();
    e.setType(Integer.parseInt(arr[0]));
    e.setTime(Short.parseShort(arr[1]));
    e.setVid(Integer.parseInt(arr[2]));
    e.setSpeed(Integer.parseInt(arr[3]));
    e.setXWay(Byte.parseByte(arr[4]));
    e.setLane(Byte.parseByte(arr[5]));
    e.setDirection(Byte.parseByte(arr[6]));
    e.setSegment(Byte.parseByte(arr[7]));
    e.setPosition(Integer.parseInt(arr[8]));
    e.setQid(arr[9]);
    e.setDay(Integer.parseInt(arr[14]));
    e.setMinute(e.time / 60 + 1);
    return e;
  }

}
