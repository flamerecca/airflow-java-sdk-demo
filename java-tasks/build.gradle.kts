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

// 從 Java 的 DAG 定義產生 Airflow 需要的 Python stub, 輸出到 dags/generated
val generateStubs by tasks.registering(JavaExec::class) {
    group = "airflow"
    description = "Generate Python stub DAG files from the Java DAG definitions"
    dependsOn("classes")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "com.example.StubGenerator"
    args(rootProject.layout.projectDirectory.dir("../dags/generated").asFile.absolutePath)
}

// bundle 由 plugin 稍後才註冊, 所以用 matching 惰性掛上
tasks.matching { it.name == "bundle" }.configureEach {
    finalizedBy(generateStubs)
}
