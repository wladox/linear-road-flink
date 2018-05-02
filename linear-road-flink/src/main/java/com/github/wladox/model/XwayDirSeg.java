package com.github.wladox.model;

import org.apache.flink.api.java.tuple.Tuple3;

public class XwayDirSeg extends Tuple3<Integer, Integer, Integer> {

  public XwayDirSeg(Integer xWay, Integer direction, Integer segment) {
    super(xWay, direction, segment);
  }
}
