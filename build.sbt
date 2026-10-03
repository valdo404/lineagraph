// Spark version is a build parameter: sbt -DsparkVersion=4.1.3 compile
val sparkVersion       = sys.props.getOrElse("sparkVersion", "4.2.0")
val openLineageVersion = "1.53.0"
val munitVersion       = "1.3.6"

ThisBuild / organization := "dev.lapoule"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

// JVM flags Spark needs on modern JDKs (module encapsulation).
val sparkJavaOptions = Seq(
  "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
  "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
  "sun.nio.ch", "sun.nio.cs", "sun.util.calendar"
).map(p => s"--add-opens=java.base/$p=ALL-UNNAMED") ++ Seq(
  "--enable-native-access=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true",
  "-Xmx2g"
)

// JVM used for run/test, also a build parameter: sbt -DjavaHome=/path/to/jdk-23 ...
// Spark 4.0.x / 4.1.x call Subject.getSubject (via Hadoop), unsupported on JDK 24+: run them on
// JDK <= 23, where the Security Manager must be explicitly allowed. Spark 4.2+ runs on JDK 25.
val forkJavaHome = sys.props.get("javaHome").map(file)

lazy val sparkSettings = Seq(
  fork                     := true,
  javaHome                 := forkJavaHome,
  javaOptions             ++= sparkJavaOptions ++ forkJavaHome.map(_ => "-Djava.security.manager=allow"),
  Test / parallelExecution := false,
  libraryDependencies      += "org.scalameta" %% "munit" % munitVersion % Test
)

lazy val root = (project in file("."))
  .aggregate(core, macros, example)
  .settings(name := "lineagraph", publish / skip := true)

lazy val core = (project in file("core"))
  .settings(
    name := "lineagraph-core",
    sparkSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"        % sparkVersion % Provided,
      "io.openlineage"    % "openlineage-java" % openLineageVersion
    )
  )

// Scala 2 def macros must be compiled before the code that expands them,
// hence a dedicated module.
lazy val macros = (project in file("macros"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "lineagraph-macros",
    sparkSettings,
    scalacOptions += "-language:experimental.macros",
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-reflect" % scalaVersion.value,
      "org.apache.spark" %% "spark-sql"     % sparkVersion % Provided
    )
  )

lazy val example = (project in file("example"))
  .dependsOn(macros)
  .settings(
    name := "lineagraph-example",
    sparkSettings,
    publish / skip      := true,
    run / baseDirectory := (ThisBuild / baseDirectory).value,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
  )
