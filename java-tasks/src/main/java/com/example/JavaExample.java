package com.example;

import org.apache.airflow.sdk.*;

@Builder.Dag(id = "java_example")
public class JavaExample {

  @Builder.Task(id = "extract")
  public long extract(Client client) {
    return 42L;
  }

  @Builder.Task(id = "transform")
  public long transform(Client client, @Builder.XCom(task = "extract") long extracted) {
    return extracted * 2;
  }

  @Builder.Task(id = "load")
  public String load(Client client, @Builder.XCom(task = "transform") long transformed) {
    var message = "Java 計算結果為 " + transformed;
    System.out.println(message);
    return message;
  }
}
