package com.github.wladox.model;

import org.apache.flink.api.java.tuple.Tuple3;

public class XwayDirSeg extends Tuple3<Byte, Byte, Byte> {

  public XwayDirSeg(Byte xWay, Byte direction, Byte segment) {
    super(xWay, direction, segment);
  }
}
