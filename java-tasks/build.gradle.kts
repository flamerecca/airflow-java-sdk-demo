plugins {
    java
    kotlin("jvm") version "2.4.21"
    kotlin("kapt") version "2.4.21"
    id("org.apache.airflow.sdk") version "1.0.0-beta1"
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    // Java 的 DAG 使用 annotationProcessor, Kotlin 的 DAG 使用 kapt
    annotationProcessor("org.apache.airflow:airflow-sdk-processor:1.0.0-beta1")
    kapt("org.apache.airflow:airflow-sdk-processor:1.0.0-beta1")
    implementation("org.apache.airflow:airflow-sdk:1.0.0-beta1")
}

airflowBundle {
    mainClass = "com.example.Main"
}
