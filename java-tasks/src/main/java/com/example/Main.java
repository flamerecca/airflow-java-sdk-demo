package com.example;

import java.util.List;
import org.apache.airflow.sdk.*;

public class Main implements BundleBuilder {
  @Override
  public Iterable<Dag> getDags() {
    return List.of(JavaExampleBuilder.build(), KotlinExampleBuilder.build());
  }

  public static void main(String[] args) {
    Server.create(args).serve(new Main().build());
  }
}
