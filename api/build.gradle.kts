import com.google.protobuf.gradle.id

plugins {
    id("java")
    id("com.google.protobuf") version "0.9.1"
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.21.7"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.54.1"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
            }
        }
    }
}

group = "io.bsrevanth2011.github"
version = "unspecified"

repositories {
    mavenCentral()
}


dependencies {
    // grpc
    implementation("io.grpc:grpc-protobuf:1.54.1")
    implementation("io.grpc:grpc-stub:1.54.1")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // necessary for Java 9+
}

tasks.test {
    useJUnitPlatform()
}