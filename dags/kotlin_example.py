from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="kotlin_example",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["demo", "java", "kotlin"],
)
def kotlin_example():
    # Kotlin 同樣透過 Java SDK 與 JavaCoordinator 執行, 所以 queue 一樣是 java
    # id 必須與 KotlinExample.kt 的 @Builder.Task 一致
    @task.stub(queue="java")
    def extract(): ...

    @task.stub(queue="java")
    def transform(extracted): ...

    @task
    def load(transformed):
        print(f"Kotlin 回傳訊息: {transformed}")

    load(transform(extract()))


kotlin_example()
