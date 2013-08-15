name := "consistent-hashing"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)

fork in Test := true

