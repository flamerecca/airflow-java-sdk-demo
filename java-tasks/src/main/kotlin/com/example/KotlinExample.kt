package com.example

import org.apache.airflow.sdk.Builder
import org.apache.airflow.sdk.Client

@Builder.Dag(id = "kotlin_example")
class KotlinExample {

    @Builder.Task(id = "extract")
    fun extract(client: Client): String = "airflow"

    @Builder.Task(id = "transform")
    fun transform(client: Client, @Builder.XCom(task = "extract") extracted: String): String =
        "Hello from Kotlin, ${extracted.uppercase()}!"
}
