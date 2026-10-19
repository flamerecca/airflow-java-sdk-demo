# airflow-java-sdk-demo

Airflow 3.3 Java SDK 範例。DAG 結構以 Python 的 `@task.stub` 宣告, task 實作寫在 Java 或 Kotlin, 由 `JavaCoordinator` 啟動 JVM 子程序執行。

![DAG 清單](docs/images/dag-list.png)

## 運作方式

1. Airflow 讀取 `dags/` 底下的 Python 檔案, 取得 DAG 的結構。
2. 標示為 `@task.stub(queue="java")` 的 task, 不會在 Python 內執行, 而是依 `queue_to_coordinator` 設定轉交給 `JavaCoordinator`。
3. `JavaCoordinator` 在 `jars_root` 內找到 bundle JAR, 啟動 JVM 執行對應的 Java 或 Kotlin 方法。
4. task 之間的回傳值透過 XCom 傳遞, 所以 Java 與 Python 的 task 可以接在同一個 DAG 裡。

stub 的 `dag_id` 與 task id 必須與 `@Builder.Dag` 和 `@Builder.Task` 的 id 完全一致, 否則 coordinator 找不到實作。

## 專案結構

| 路徑 | 說明 |
| :--- | :--- |
| `dags/hello_dag.py` | 純 Python 的通用範例 DAG |
| `dags/java_example.py` | Java 範例的 Python stub, `extract` 與 `transform` 由 Java 執行, `load` 由 Python 執行 |
| `dags/kotlin_example.py` | Kotlin 範例的 Python stub, 同樣透過 Java SDK 執行 |
| `java-tasks/` | Java 與 Kotlin 的 task 實作, 使用 Gradle 建置, 兩個 DAG 打包在同一個 bundle |

## 先決條件

- Python 3.14 與 [uv](https://docs.astral.sh/uv/)
- JDK 17 以上
- Gradle

## 執行步驟

### 1. 安裝 Python 依賴

```bash
uv sync
```

### 2. 建置 Java bundle

```bash
cd java-tasks
gradle bundle
```

產出位於 `java-tasks/build/bundle/java-tasks-all.jar`。Kotlin 的 annotation processor 透過 `kapt` 執行。

### 3. 設定 Airflow

先產生並設定 `airflow.cfg`, 專案的 `airflow.cfg` 不納入版控。在其中加入以下設定, `jars_root` 請改成實際的絕對路徑:

```ini
[sdk]
coordinators = {"java-jdk17": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator", "kwargs": {"jars_root": ["/absolute/path/to/airflow-java-sdk-demo/java-tasks/build/bundle"]}}}
queue_to_coordinator = {"java": "java-jdk17"}
```

### 4. 啟動 Airflow

```bash
export AIRFLOW_HOME=$PWD
export PATH=$PWD/.venv/bin:$PATH
airflow standalone
```

- 必須設定 `AIRFLOW_HOME`, 否則 Airflow 會改讀 `~/airflow`, 而不是這個專案的 `airflow.cfg`。
- `standalone` 會呼叫 PATH 上的 `airflow`, 所以也要把 `.venv/bin` 加進 `PATH`。
- UI 位於 `http://localhost:8080`, 帳號為 `admin`, 密碼在 `simple_auth_manager_passwords.json.generated`。

### 5. 觸發 DAG

在 UI 的 Dags 頁面點選 `java_example` 或 `kotlin_example`, 再按右上角的「觸發」。也可以使用指令列:

```bash
airflow dags unpause java_example
airflow dags trigger java_example
```

## 預期結果

### `java_example`

`extract` 回傳 42, `transform` 乘以 2, Python 的 `load` 印出「Java 計算結果為 84」。

![java_example 的 load 日誌](docs/images/java-load-log.png)

### `kotlin_example`

`extract` 回傳 `"airflow"`, `transform` 回傳 `"Hello from Kotlin, AIRFLOW!"`。下圖的任務清單中, `extract` 與 `transform` 的任務操作器顯示為 `@task.stub`, 表示它們是交給 Java SDK 執行, `load` 則是一般的 `@task`。

![kotlin_example 的執行頁](docs/images/kotlin-run.png)

Python 的 `load` 印出 Kotlin 回傳的訊息:

![kotlin_example 的 load 日誌](docs/images/kotlin-load-log.png)

## 疑難排解

| 現象 | 原因與處理 |
| :--- | :--- |
| `airflow dags test` 出現 `@task.stub should not be executed directly` | `dags test` 在同一個行程內執行 task, 不會經過 coordinator, stub 無法用它驗證。請改用 `airflow standalone` 並觸發 DAG。 |
| `airflow` 指令提示 `Database migration required` | 先執行 `airflow db migrate`。 |
| `airflow standalone` 的子行程啟動失敗 | `airflow` 不在 `PATH`, 請先 `export PATH=$PWD/.venv/bin:$PATH`。 |
| 8080 Port 已被占用 | 先前的 Airflow 行程還在執行, 請先停止, 修改過設定後也需要重啟才會生效。 |
| task 找不到 JAR 或 `Main-Class` | 確認已執行 `gradle bundle`, 且 `jars_root` 是正確的絕對路徑。 |
| 改了 Java 或 Kotlin 程式碼後沒有生效 | 重新執行 `gradle bundle` 即可, 每個 task 都會啟動新的 JVM, 不需要重啟 Airflow。 |

## 注意事項

Java SDK 目前為 `1.0.0-beta1`, API 之後可能會變動。官方文件請參考 [Java SDK](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html)。
