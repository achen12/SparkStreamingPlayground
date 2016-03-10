name := "sparkstreamingplayground"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"

mainClass in (Compile, packageBin) := Some("streamingtests.twitter.TwitterScrapper")
