from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="java_example",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["demo", "java"],
)
def java_example():
    # extract 與 transform 的實作在 java-tasks 專案, id 必須與 Java 端的 @Builder.Task 一致
    @task.stub(queue="java")
    def extract(): ...

    @task.stub(queue="java")
    def transform(extracted): ...

    @task
    def load(transformed):
        print(f"Java 計算結果為 {transformed}")

    load(transform(extract()))


java_example()
