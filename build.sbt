import scala.sys.process.Process
import scala.io.Source

import Dependencies._

// Load Zilliz Nexus credentials
credentials ++= {
  val realm = "Sonatype Nexus Repository Manager"
  val host = "nexus.zilliz.cc"

  println(s"[Build] Checking Nexus credentials for $host ($realm)...")

  // 1. Try Environment Variables first (CI/CD friendly)
  val envCreds = (sys.env.get("NEXUS_USER"), sys.env.get("NEXUS_PASSWORD")) match {
    case (Some(u), Some(p)) =>
      println("[Build] Found NEXUS_USER and NEXUS_PASSWORD in environment variables.")
      Seq(Credentials(realm, host, u, p))
    case _ =>
      println("[Build] NEXUS_USER/NEXUS_PASSWORD not set in environment.")
      Seq.empty
  }

  // 2. Try File-based credentials
  val fileCreds = {
    val credFile = List(
      Path.userHome / ".sbt" / "zilliz_nexus_credentials",
      file(".sbt/zilliz_nexus_credentials")
    ).find(_.exists)
      .getOrElse(Path.userHome / ".sbt" / "zilliz_nexus_credentials")

    println(s"[Build] Checking credentials file: $credFile")

    if (credFile.exists) {
      println(s"[Build] Loading credentials from file: $credFile")
      try {
        Seq(Credentials(credFile))
      } catch {
        case e: Exception =>
          println(s"[Build] ERROR: Failed to define credentials from file: ${e.getMessage}")
          e.printStackTrace()
          Seq.empty
      }
    } else {
      val fallback = Path.userHome / ".sbt" / "sonatype.credentials"
      println(s"[Build] Primary credentials file not found. Fallback to: $fallback")
      Seq(Credentials(fallback))
    }
  }

  val finalCreds = envCreds ++ fileCreds
  println(s"[Build] Total credential sets loaded: ${finalCreds.size}")
  finalCreds
}

ThisBuild / organizationName := "zilliz"
ThisBuild / organizationHomepage := Some(url("https://zilliz.com/"))
// For cross-compiling (if applicable)
// crossScalaVersions := Seq("2.12.x", "2.13.x")
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / description := "Milvus Spark Connector to use in Spark ETLs to populate a Milvus vector database."
ThisBuild / versionScheme := Some("early-semver")

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true

// Publish to internal Nexus repository
ThisBuild / publishTo := {
  val nexusSnapshots = "https://nexus.zilliz.cc/repository/maven-snapshots"
  if (isSnapshot.value)
    Some("Sonatype Nexus Repository Manager" at nexusSnapshots)
  else None // Or configure a release repository if needed
}

ThisBuild / licenses := List(
  "Server Side Public License v1" -> new URL(
    "https://raw.githubusercontent.com/mongodb/mongo/refs/heads/master/LICENSE-Community.txt"
  ),
  "GNU Affero General Public License v3 (AGPLv3)" -> new URL(
    "https://www.gnu.org/licenses/agpl-3.0.txt"
  )
)
ThisBuild / homepage := Some(
  url("https://github.com/zilliztech/milvus-spark-connector")
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/zilliztech/milvus-spark-connector"),
    "scm:git@github.com:zilliztech/milvus-spark-connector.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "santiago-wjq",
    name = "Santiago Wu",
    email = "santiago.wu@zilliz.com",
    url = url("https://github.com/santiago-wjq")
  )
)

lazy val arch = System.getProperty("os.arch") match {
  case "amd64" | "x86_64"  => "amd64"
  case "aarch64" | "arm64" => "arm64"
  case other               => other
}

// Get git branch name from env var (for Docker builds) or git command, sanitize for Maven version
lazy val gitBranch = {
  val branch = sys.env.getOrElse(
    "GIT_BRANCH",
    scala.util
      .Try(Process("git rev-parse --abbrev-ref HEAD").!!.trim)
      .getOrElse("unknown")
  )
  // Replace invalid characters for Maven version (only alphanumeric, dash, dot, underscore allowed)
  branch.replaceAll("[^a-zA-Z0-9._-]", "-")
}

lazy val root = (project in file("."))
  .settings(
    name := "spark-connector",
    assembly / parallelExecution := true,
    Test / parallelExecution := true,
    Compile / compile / parallelExecution := true,
    version := s"0.1.0-${gitBranch}-${arch}-SNAPSHOT", // SNAPSHOT version for Nexus
    organization := "com.zilliz",

    // Disable Scaladoc and sources jar for publish (not needed for internal Nexus, speeds up build)
    Compile / packageDoc / publishArtifact := false,
    Compile / packageSrc / publishArtifact := false,

    // Fork JVM for run and tests to properly load native libraries
    run / fork := true,
    Test / fork := true,

    // Show test logs immediately (don't buffer)
    Test / logBuffered := false,

    // JVM options for run
    run / javaOptions ++= Seq(
      "-Xss2m",
      "-Djava.library.path=.",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    run / envVars := Map(
      "LD_PRELOAD" -> (baseDirectory.value / s"src/main/resources/native/libmilvus-storage.so").getAbsolutePath
    ),

    // Include test dependencies in run classpath for example applications
    Compile / run / fullClasspath := (Compile / run / fullClasspath).value ++ (Test / fullClasspath).value,

    // JVM options for tests
    Test / javaOptions ++= Seq(
      "-Xss2m",
      "-Xmx4g",
      "-Djava.library.path=.",
      "-Dlog4j2.configurationFile=log4j2.properties",
      "-Dlog4j2.debug=true",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    ),
    Test / envVars := Map(
      "LD_PRELOAD" -> (baseDirectory.value / s"src/main/resources/native/libmilvus-storage.so").getAbsolutePath
    ),

    // Add milvus-storage JNI library as unmanaged dependency
    Compile / unmanagedJars += baseDirectory.value / "milvus-storage" / "java" / "target" / "scala-2.13" / "milvus-storage-jni-test_2.13-0.1.0-SNAPSHOT.jar",
    Test / unmanagedJars += baseDirectory.value / "milvus-storage" / "java" / "target" / "scala-2.13" / "milvus-storage-jni-test_2.13-0.1.0-SNAPSHOT.jar",
    libraryDependencies ++= Seq(
      munit % Test,
      scalaTest % Test,
      grpcNetty,
      scalapbRuntime % "protobuf",
      scalapbRuntimeGrpc,
      scalapbCompilerPlugin,
      sparkCore,
      sparkSql,
      sparkCatalyst,
      sparkMLlib,
      parquetHadoop,
      hadoopCommon,
      hadoopAws,
      awsSdkS3,
      awsSdkS3Transfer,
      awsSdkCore,
      jacksonScala,
      jacksonDatabind,
      arrowFormat,
      arrowVector,
      arrowMemoryCore,
      arrowMemoryNetty,
      arrowCData
    ),
    Compile / PB.protoSources += baseDirectory.value / "milvus-proto/proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    Compile / unmanagedSourceDirectories += (
      Compile / PB.targets
    ).value.head.outputPath,
    Compile / packageBin / mappings ++= {
      val base = (Compile / PB.targets).value.head.outputPath
      (base ** "*.scala").get.map { file =>
        file -> s"generated_protobuf/${file.relativeTo(base).getOrElse(file)}"
      }
    },
    Compile / resourceDirectories += baseDirectory.value / "src" / "main" / "resources",
    // 发布 assembly JAR 作为单独的 artifact，带 classifier
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(assembly / artifact, assembly)
  )

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shade_proto.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "shade_googlecommon.@1").inAll
  // Note: Arrow cannot be shaded due to JNI bindings with hardcoded class names
  // Use spark.driver.userClassPathFirst=true to prioritize our Arrow version
)

assembly / assemblyMergeStrategy := {
  case PathList("native", xs @ _*) => MergeStrategy.first
  // Handle all Netty native-image files
  case PathList("META-INF", "native-image", "io.netty", _*) =>
    MergeStrategy.discard
  // Handle Netty version properties
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.discard
  // Handle mime.types
  case PathList("mime.types") =>
    MergeStrategy.filterDistinctLines
  // Handle FastDoubleParser notice
  case PathList("META-INF", "FastDoubleParser-NOTICE") =>
    MergeStrategy.discard
  // Handle Arrow git properties
  case PathList("arrow-git.properties") =>
    MergeStrategy.first
  // Handle module-info.class files
  case x if x.endsWith("module-info.class") =>
    MergeStrategy.discard
  // Handle hadoop package-info conflicts
  case PathList("org", "apache", "hadoop", xs @ _*)
      if xs.last == "package-info.class" =>
    MergeStrategy.first
  // Handle AWS SDK VersionInfo conflicts
  case PathList("software", "amazon", "awssdk", xs @ _*)
      if xs.last == "VersionInfo.class" =>
    MergeStrategy.first
  // Default case
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// import scalapb.compiler.Version
// val grpcJavaVersion =
//   SettingKey[String]("grpcJavaVersion", "ScalaPB gRPC Java version")
// grpcJavaVersion := Version.grpcJavaVersion

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
