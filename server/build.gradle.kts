plugins {
    id("java")
}

group = "io.bsrevanth2011.github"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    implementation(project(":api"))

    // grpc
    runtimeOnly("io.grpc:grpc-netty-shaded:1.54.1")
    implementation("io.grpc:grpc-protobuf:1.54.1")
    implementation("io.grpc:grpc-stub:1.54.1")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // necessary for Java 9+

    // rocksdb
    implementation("org.rocksdb:rocksdbjni:8.1.1.1")

    // logging
    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.4.7")

    // utility libraries
    compileOnly("org.projectlombok:lombok:1.18.28")
    annotationProcessor("org.projectlombok:lombok:1.18.28")

    testCompileOnly("org.projectlombok:lombok:1.18.28")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.28")

    implementation("org.eclipse.collections:eclipse-collections:11.1.0")
    implementation("org.apache.commons:commons-configuration2:2.9.0")
    implementation("org.yaml:snakeyaml:2.0")

    // test
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}