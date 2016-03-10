import sbt.Resolver

//assemblyJarName in assembly := "H5Spark.jar"

name := "h5spark"

version := "1.0"

scalaVersion := "2.10.5"

resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"

//mainClass in (Compile,run) := Some("org.nersc.io.read")
mainClass in (Compile,run) := Some("org.nersc.io.write")
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"
  //"com.github.fommil.netlib" % "all" % "1.1.2",
 // "org.apache.commons" % "commons-compress" % "1.5",
   //"org.hdfgroup" % "hdf-java" % "2.6.1",
   //"org.scala-saddle" % "jhdf5" % "2.9",
   // "org.scala-saddle" % "saddle-hdf5_2.11" % "1.3.4",
   // "log4j" % "log4j" % "1.2.14"
)

/**
 * Prevents multiple SparkContexts from being launched
 */
parallelExecution in Test := false

//test in assembly := {}



