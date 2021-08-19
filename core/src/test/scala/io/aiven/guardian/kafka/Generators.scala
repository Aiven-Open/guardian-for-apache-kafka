package io.aiven.guardian.kafka

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Gen

import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object Generators {
  def baseReducedConsumerRecordGen(topic: String,
                                   offset: Long,
                                   key: String,
                                   timestamp: Long
  ): Gen[ReducedConsumerRecord] = for {
    t             <- Gen.const(topic)
    o             <- Gen.const(offset)
    k             <- Gen.const(key)
    value         <- Gen.alphaStr.map(string => Base64.getEncoder.encodeToString(string.getBytes))
    ts            <- Gen.const(timestamp)
    timestampType <- Gen.const(TimestampType.CREATE_TIME)
  } yield ReducedConsumerRecord(
    t,
    o,
    k,
    value,
    ts,
    timestampType
  )

  private val keyOffsetMap = new ConcurrentHashMap[String, AtomicLong]().asScala

  def createOffsetsByKey(key: String): Long = {
    val returned = keyOffsetMap.getOrElseUpdate(key, new AtomicLong())
    returned.incrementAndGet()
  }

  /** A generator that allows you to generator an arbitrary collection of Kafka `ReducedConsumerRecord` used for
    * mocking. The generator will create a random distribution of keys (with each key having its own specific record of
    * offsets) allowing you to mock multi partition Kafka scenarios.
    * @param topic
    *   The name of the kafka topic
    * @param min
    *   The minimum number of `ReducedConsumerRecord`'s to generate
    * @param max
    *   The maximum number of `ReducedConsumerRecord`'s to generate
    * @param padTimestampsMillis
    *   The amount of padding (in milliseconds) between consecutive timestamps. If set to 0 then all timestamps will
    *   differ by a single millisecond
    * @return
    *   A list of generated `ReducedConsumerRecord`
    */
  def kafkaReducedConsumerRecordsGen(topic: String,
                                     min: Int,
                                     max: Int,
                                     padTimestampsMillis: Int
  ): Gen[List[ReducedConsumerRecord]] = for {
    t                           <- Gen.const(topic)
    numberOfTotalReducedRecords <- Gen.chooseNum[Int](min, max)
    numberOfKeys                <- Gen.chooseNum[Int](1, numberOfTotalReducedRecords)
    keys <- Gen.listOfN(numberOfKeys, Gen.alphaStr.map(string => Base64.getEncoder.encodeToString(string.getBytes)))
    keyDistribution <- Gen.listOfN(numberOfTotalReducedRecords, Gen.oneOf(keys))
    keysWithOffSets = keyDistribution.groupMap(identity)(createOffsetsByKey)
    reducedConsumerRecordsWithoutTimestamp = keysWithOffSets
                                               .map { case (key, offsets) =>
                                                 offsets.map { offset =>
                                                   (t, offset, key)
                                                 }
                                               }
                                               .toList
                                               .flatten
    timestampsWithPadding <- Gen
                               .sequence((1 to reducedConsumerRecordsWithoutTimestamp.size).map { _ =>
                                 Gen.chooseNum[Long](1, padTimestampsMillis + 1)
                               })
                               .map(_.asScala.toList)
    timestamps = timestampsWithPadding.foldLeft(ListBuffer(1L)) { case (timestamps, padding) =>
                   val last = timestamps.last
                   timestamps.append(last + padding)
                 }

    reducedConsumerRecords <-
      Gen
        .sequence(reducedConsumerRecordsWithoutTimestamp.zip(timestamps).map { case ((topic, offset, key), timestamp) =>
          baseReducedConsumerRecordGen(topic, offset, key, timestamp)
        })
        .map(_.asScala.toList)

  } yield reducedConsumerRecords

}
