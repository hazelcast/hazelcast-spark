logLevel := Level.Warn
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("no.arktekk.sbt" % "aether-deploy" % "0.17")
testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")

