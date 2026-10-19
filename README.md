# airflow-java-sdk-demo

Airflow 3.3 Java SDK 範例。DAG 結構以 Python 的 `@task.stub` 宣告, task 實作寫在 Java。

## 專案結構

- `dags/hello_dag.py`: 純 Python 的通用範例 DAG
- `dags/java_example.py`: Java 範例 DAG 的 Python stub, `extract` 與 `transform` 由 Java 執行, `load` 由 Python 執行
- `dags/kotlin_example.py`: Kotlin 範例 DAG 的 Python stub, 同樣透過 Java SDK 執行
- `java-tasks/`: Java 與 Kotlin 的 task 實作, 使用 Gradle 建置, 兩個 DAG 打包在同一個 bundle

stub 的 task id 必須與 Java 端 `@Builder.Task` 的 id 一致, `dag_id` 也必須與 `@Builder.Dag` 一致。

## 建置 Java bundle

需要 JDK 17 以上與 Gradle。Kotlin 的 annotation processor 透過 `kapt` 執行。

```bash
cd java-tasks
gradle bundle
```

產出位於 `java-tasks/build/bundle/`。

## 設定 Airflow

在 `airflow.cfg` 加入以下設定, `jars_root` 請改成實際的絕對路徑:

```ini
[sdk]
coordinators = {
  "java-jdk17": {
    "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
    "kwargs": {"jars_root": ["/absolute/path/to/airflow-java-sdk-demo/java-tasks/build/bundle"]}
  }
}
queue_to_coordinator = {"java": "java-jdk17"}
```

## 執行

```bash
export AIRFLOW_HOME=$PWD
airflow standalone
```

接著在 UI 手動觸發 `java_example` 或 `kotlin_example`。
