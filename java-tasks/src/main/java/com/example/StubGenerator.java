package com.example;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.airflow.sdk.Builder;
import org.apache.airflow.sdk.Dag;
import org.apache.airflow.sdk.Task;

/**
 * 從 Java 端的 DAG 定義產生 Airflow 需要的 Python stub 檔案。
 *
 * Airflow 3.3 只認得 Python 定義的 DAG, 而 Java SDK 1.0.0-beta1 的 Dag 物件只記錄 DAG id 與 task,
 * 不含依賴關係與排程。因此這裡以反射讀取 @Builder.Task 與 @Builder.XCom, 由 XCom 的來源推得依賴,
 * 再輸出 stub, 讓使用者不需要手寫任何 Python。
 */
public class StubGenerator {

  private static final String HEADER =
      """
      # 此檔案由 StubGenerator 從 Java 的 DAG 定義自動產生, 請勿手動修改。
      # 修改請改 java-tasks 內的原始碼, 再執行 gradle bundle 重新產生。
      from __future__ import annotations

      import pendulum
      from airflow.sdk import dag, task

      """;

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("用法: StubGenerator <輸出目錄>");
    }
    Path outDir = Path.of(args[0]);
    Files.createDirectories(outDir);
    deleteStaleStubs(outDir);

    for (Dag dag : new Main().getDags()) {
      Path file = outDir.resolve(dag.getId() + ".py");
      Files.writeString(file, render(dag));
      System.out.println("已產生 " + file);
    }
  }

  private static void deleteStaleStubs(Path outDir) throws IOException {
    try (DirectoryStream<Path> stale = Files.newDirectoryStream(outDir, "*.py")) {
      for (Path p : stale) {
        Files.delete(p);
      }
    }
  }

  /** task id 對應到它依序依賴的上游 task id。 */
  private static Map<String, List<String>> readDependencies(Dag dag) throws Exception {
    Map<String, List<String>> deps = new LinkedHashMap<>();
    for (Map.Entry<String, Class<? extends Task>> entry : dag.getTasks$sdk().entrySet()) {
      deps.put(entry.getKey(), upstreamOf(entry.getKey(), entry.getValue()));
    }
    return deps;
  }

  private static List<String> upstreamOf(String taskId, Class<? extends Task> builderTask) {
    // 註解處理器產生的 task 是 <使用者類別>Builder 的內部類別, 以此慣例找回使用者的類別
    Class<?> builder = builderTask.getEnclosingClass();
    String builderName = builder.getName();
    if (!builderName.endsWith("Builder")) {
      throw new IllegalStateException("無法由 " + builderName + " 推得使用者類別");
    }
    Class<?> userClass;
    try {
      userClass = Class.forName(builderName.substring(0, builderName.length() - "Builder".length()));
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("找不到使用者類別: " + builderName, e);
    }

    for (Method method : userClass.getDeclaredMethods()) {
      Builder.Task marker = method.getAnnotation(Builder.Task.class);
      if (marker == null || !marker.id().equals(taskId)) {
        continue;
      }
      List<String> upstream = new ArrayList<>();
      for (Annotation[] annotations : method.getParameterAnnotations()) {
        for (Annotation annotation : annotations) {
          if (annotation instanceof Builder.XCom xcom) {
            upstream.add(xcom.task());
          }
        }
      }
      return upstream;
    }
    throw new IllegalStateException(userClass.getName() + " 找不到 id 為 " + taskId + " 的 @Builder.Task");
  }

  private static List<String> topologicalOrder(String dagId, Map<String, List<String>> deps) {
    for (Map.Entry<String, List<String>> e : deps.entrySet()) {
      for (String up : e.getValue()) {
        if (!deps.containsKey(up)) {
          throw new IllegalStateException(dagId + " 的 task " + e.getKey() + " 依賴不存在的 task: " + up);
        }
      }
    }
    Set<String> done = new LinkedHashSet<>();
    while (done.size() < deps.size()) {
      boolean progressed = false;
      for (Map.Entry<String, List<String>> e : deps.entrySet()) {
        if (!done.contains(e.getKey()) && done.containsAll(e.getValue())) {
          done.add(e.getKey());
          progressed = true;
        }
      }
      if (!progressed) {
        throw new IllegalStateException(dagId + " 的 task 依賴形成循環");
      }
    }
    return new ArrayList<>(done);
  }

  private static String pythonName(String taskId) {
    return taskId.replaceAll("[^A-Za-z0-9_]", "_");
  }

  private static String render(Dag dag) throws Exception {
    Map<String, List<String>> deps = readDependencies(dag);
    List<String> order = topologicalOrder(dag.getId(), deps);

    StringBuilder py = new StringBuilder(HEADER);
    py.append("\n@dag(\n")
        .append("    dag_id=\"").append(dag.getId()).append("\",\n")
        .append("    schedule=None,\n")
        .append("    start_date=pendulum.datetime(2026, 1, 1, tz=\"Asia/Taipei\"),\n")
        .append("    catchup=False,\n")
        .append("    tags=[\"java-sdk\", \"generated\"],\n")
        .append(")\n")
        .append("def ").append(pythonName(dag.getId())).append("():\n");

    for (String taskId : order) {
      List<String> params = deps.get(taskId).stream().map(StubGenerator::pythonName).toList();
      py.append("    @task.stub(task_id=\"").append(taskId).append("\", queue=\"java\")\n")
          .append("    def ").append(pythonName(taskId)).append("(")
          .append(String.join(", ", params)).append("): ...\n\n");
    }

    for (String taskId : order) {
      List<String> args = deps.get(taskId).stream().map(up -> "r_" + pythonName(up)).toList();
      py.append("    r_").append(pythonName(taskId)).append(" = ")
          .append(pythonName(taskId)).append("(").append(String.join(", ", args)).append(")\n");
    }

    py.append("\n\n").append(pythonName(dag.getId())).append("()\n");
    return py.toString();
  }
}
