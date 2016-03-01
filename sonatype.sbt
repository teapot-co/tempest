// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "co.teapot"

// To sync with Maven central, you need to supply the following information:
pomExtra in Global := {
  <url>https://github.com/teapot-co/tempest/</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/teapot-co/tempest.git</connection>
      <developerConnection>scm:git:git@github.com:teapot-co/tempest.git</developerConnection>
      <url>github.com/teapot-co/teapot-co/tempest.git</url>
    </scm>
    <developers>
      <developer>
        <id>plofgren</id>
        <name>Peter Lofgren</name>
        <url>http://cs.stanford.edu/~plofgren</url>
      </developer>
    </developers>
}
