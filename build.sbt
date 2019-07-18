
val json4sVersion = "3.6.0-M3"
val scioVersion = "0.7.4"
val beamVersion = "2.11.0"
val slf4jVersion = "1.7.25"
val kafkaVersion = "2.0.0"
val scalaMacrosVersion = "2.1.1"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "com.arquivei",
  version := "0.1.0",
  scalaVersion := "2.12.8",
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
  ),
  javacOptions ++= Seq(
    "-source", "1.8",
    "-target", "1.8"
  ),
  testOptions in Test += Tests.Argument("-oDF")
)


lazy val macroSettings = Seq(
  resolvers += Resolver.sonatypeRepo("releases"),
  addCompilerPlugin("org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full)
)

lazy val core = (project in file("./shared/core"))
  .settings(
    commonSettings ++ macroSettings,
    name := "Core",
    libraryDependencies ++= Seq(
      "org.yaml" % "snakeyaml" % "1.23",
      "org.json4s" %% "json4s-jackson" % json4sVersion,
      "com.google.cloud" % "google-cloud-bigquery" % "1.44.0",
      "org.slf4j" % "slf4j-jdk14" % slf4jVersion,
      "com.google.apis" % "google-api-services-bigquery" % "v2-rev374-1.23.0",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "com.nrinaudo" %% "kantan.csv" % "0.4.0",
      "com.softwaremill.sttp" %% "core" % "1.3.5"
    )
  )

lazy val kafkagenericeventparser = (project in file("./stream/kafkagenericeventparser"))
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonSettings ++ macroSettings,
    name := "KafkaGenericEventParser",
    packageName in Docker := "arquivei/kafkagenericeventparser",
    version in Docker := "1907.1",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "com.spotify" %% "scio-bigquery" % scioVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-kafka" % beamVersion,
      "org.apache.kafka" %% "kafka" % kafkaVersion,
    )
  )

lazy val pubsubgenericeventparser = (project in file("./stream/pubsubgenericeventparser"))
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonSettings ++ macroSettings,
    name := "PubsubGenericEventParser",
    packageName in Docker := "arquivei/pubsubgenericeventparser",
    version in Docker := "1907.1",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "com.spotify" %% "scio-bigquery" % scioVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
    )
  )
