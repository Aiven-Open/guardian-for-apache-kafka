package io.aiven.guardian.kafka.gcs

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

class FakeGCSContainer
    extends GenericContainer(
      "fsouza/fake-gcs-server",
      exposedPorts = List(4443),
      waitStrategy = Some(Wait.forHttp("/storage/v1/b").forPort(4443).withStartupTimeout(Duration.ofSeconds(10))),
      command = List("-scheme", "http")
    ) {

  def getHostAddress: String =
    container.getContainerIpAddress + ":" + container.getMappedPort(4443)
}
