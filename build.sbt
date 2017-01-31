name := "tempest"

version := "0.14.0"

organization := "co.teapot"

description := "A graph library and database which efficiently supports dynamic graphs with billions of edges"

scalaVersion := "2.11.5"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "net.openhft" % "koloboke-impl-jdk6-7" % "0.6.6"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "6.6.0"

libraryDependencies += "org.apache.thrift" % "libthrift" % "0.9.1"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.6.1" // Needed for thrift

libraryDependencies += "com.twitter" %% "util-app" % "6.23.0"

libraryDependencies += "com.twitter" %% "util-logging" % "6.23.0"

libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1207"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.11"

libraryDependencies += "com.typesafe.play" %% "anorm" % "2.4.0"

libraryDependencies += "com.zaxxer" % "HikariCP" % "2.4.5"

libraryDependencies += "com.indeed" % "util-mmap" % "1.0.20"

scalacOptions += "-target:jvm-1.8" // Anorm requires 1.8

scalacOptions += "-Ywarn-unused"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

unmanagedSourceDirectories in Compile += baseDirectory.value / "src/gen/java"

test in assembly := {} // Disable tests during assembly

assemblyJarName in assembly := "tempest-assembly.jar" // By default, assembly appends a version number, which requires changing bash scripts that reference the jar
