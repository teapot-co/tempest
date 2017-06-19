name := "tempest"
version := "0.14.0"
organization := "co.teapot"
description := "A graph library and database which efficiently supports dynamic graphs with billions of edges"
scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
  "net.openhft" % "koloboke-impl-jdk6-7" % "0.6.6",
  "it.unimi.dsi" % "fastutil" % "6.6.0",
  "org.apache.thrift" % "libthrift" % "0.9.1",
  "org.slf4j" % "slf4j-log4j12" % "1.6.1", // Needed for thrift
  "com.twitter" %% "util-app" % "6.23.0",
  "com.twitter" %% "util-logging" % "6.23.0",
  "org.postgresql" % "postgresql" % "9.4.1207",
  "org.yaml" % "snakeyaml" % "1.11",
  "com.typesafe.play" %% "anorm" % "2.4.0",
  "com.zaxxer" % "HikariCP" % "2.4.5",
  "com.indeed" % "util-mmap" % "1.0.20",
  "com.h2database" % "h2" % "1.4.196" /*% "test"*/
)

scalacOptions ++= Seq(
  "-target:jvm-1.8", // Anorm requires 1.8
  "-Ywarn-unused"
)

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

test in assembly := {} // Disable tests during assembly

// don't reuse the same JVM instance between tests runs
// we're currently leaking some resources so if we reuse the same JVM instance
// tests fail with errors related to obtaining a database driver for postgres
fork in Test := true

assemblyJarName in assembly := "tempest-assembly.jar" // By default, assembly appends a version number, which requires changing bash scripts that reference the jar

sourceGenerators in Compile += Def.task {

  val generateThriftScript = baseDirectory.value / "src/main/bash/generate_thrift.sh"

  val cachedProcess = FileFunction.cached(streams.value.cacheDirectory / "generate_thrift", FilesInfo.hash) { (in: Set[File]) =>
    // here we don't extract input files
    // we rely on bash script to know which thrift files to Process
    val generatorOutputPath = (sourceManaged in Compile).value / "thrift"
    Process(
      command = generateThriftScript.toString,
      arguments = Seq(generatorOutputPath.toString)
    ).!
    (generatorOutputPath ** "*.java").get.toSet
  }
  // we pass both the path to bash script and paths to all *.thrift files in src/main/thrift directory
  // so cache gets invalidated when _either_ the bash script itself is edited or one of thrift files
  // is changed
  cachedProcess(
    Set(generateThriftScript)
    ++ ((sourceDirectory in Compile in compile).value / "thrift" ** "*.thrift").get
  ).toSeq
}.taskValue
