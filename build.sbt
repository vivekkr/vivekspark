name := "vivekspark"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

lazy val akkaV = "2.4.9"
lazy val scalaTestV = "3.0.1"
lazy val mockitoV = "2.8.47"
lazy val slickV = "3.1.1"
lazy val sparkV = "2.1.0"

lazy val akka = Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-stream" % akkaV,
  "com.typesafe.akka" %% "akka-http-experimental" % akkaV,
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaV,

  "com.typesafe.akka" %% "akka-http-testkit" % akkaV
)

lazy val slickDeps = Seq(
  "com.typesafe.slick" %% "slick" % slickV,
  "com.typesafe.slick" %% "slick-hikaricp" % slickV
)

lazy val quicklensDeps = Seq(
  "com.softwaremill.quicklens" %% "quicklens" % "1.4.8"
)

lazy val bcrypt = Seq(
  "com.github.t3hnar" %% "scala-bcrypt" % "3.0"
)

lazy val postgresDeps = Seq(
  "org.postgresql" % "postgresql" % "9.4.1208"
)

lazy val h2TestDeps = Seq(
  "com.h2database" % "h2" % "1.4.192" % "test"
)

lazy val scalaTestAndMockito = Seq(
  "org.scalatest" %% "scalatest" % scalaTestV % "test",
  "org.mockito" % "mockito-core" % mockitoV % "test"
)

lazy val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkV,
  "org.apache.spark" %% "spark-sql" % sparkV,
  "org.apache.spark" %% "spark-hive" % sparkV,
  "org.apache.spark" %% "spark-streaming" % sparkV,
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0"
)

lazy val elasticSearch = Seq(
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.4.5",
  "org.elasticsearch" % "elasticsearch-spark_2.11" % "2.4.5"
)

libraryDependencies ++= akka ++ quicklensDeps ++ scalaTestAndMockito ++ sparkDeps ++ elasticSearch

lazy val commonSettings = Seq(
  organization := "com.vivek",
  version := "1.0",
  scalaVersion := "2.12.1",
  scalacOptions := Seq("-unchecked", "-feature", "-deprecation", "-encoding", "utf8"),
  resolvers ++= Seq(
    "Amazon" at "http://dynamodb-local.s3-website-us-west-2.amazonaws.com/release",
    "JBoss" at "https://repository.jboss.org/",
    Resolver.sonatypeRepo("public")
  )
)

lazy val root = Project(id = "vivekspark",
  base = file("."),
  settings = commonSettings ++ Seq(name := "vivekspark")
)
