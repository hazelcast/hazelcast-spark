logLevel := Level.Warn
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.1")
testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")

