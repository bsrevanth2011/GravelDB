plugins {
    id("java")
}

group = "io.bsrevanth2011.github"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // protos
    implementation(project(":api"))

    // utilities
    implementation("org.apache.commons:commons-configuration2:2.9.0")
    implementation("org.yaml:snakeyaml:2.0")

    // grpc
    runtimeOnly("io.grpc:grpc-netty-shaded:1.54.1")
    implementation("io.grpc:grpc-protobuf:1.54.1")
    implementation("io.grpc:grpc-stub:1.54.1")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // necessary for Java 9+

    // logging
    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.4.7")

    // utility libraries
    implementation("org.eclipse.collections:eclipse-collections:11.1.0")
    implementation("org.apache.commons:commons-configuration2:2.9.0")

    // test
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}