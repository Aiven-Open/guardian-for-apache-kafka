package io.aiven.guardian.kafka.s3

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

class MinioContainer(accessKey: String, secretKey: String)
    extends GenericContainer(
      "minio/minio",
      exposedPorts = List(9000),
      waitStrategy = Some(Wait.forHttp("/minio/health/ready").forPort(9000).withStartupTimeout(Duration.ofSeconds(10))),
      command = List("server", "/data"),
      env = Map(
        "MINIO_ACCESS_KEY" -> accessKey,
        "MINIO_SECRET_KEY" -> secretKey
      )
    ) {

  def getHostAddress: String =
    s"http://${container.getContainerIpAddress}:${container.getMappedPort(9000)}"
}
