package io.aiven.guardian.kafka

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Gen

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.MILLISECONDS
import scala.jdk.CollectionConverters._

import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

object Generators {
  def baseReducedConsumerRecordGen(topic: String,
                                   offset: Long,
                                   key: String,
                                   timestamp: Long
  ): Gen[ReducedConsumerRecord] = for {
    t         <- Gen.const(topic)
    p         <- Gen.const(0) // In mocks this value is irrelevant and in tests we always have a single partition
    o         <- Gen.const(offset)
    k         <- Gen.const(key)
    valueSize <- Gen.choose(1, 100)
    value <- Gen.stringOfN(valueSize, Gen.alphaChar).map(string => Base64.getEncoder.encodeToString(string.getBytes))
    ts    <- Gen.const(timestamp)
    timestampType <- Gen.const(TimestampType.CREATE_TIME)
  } yield ReducedConsumerRecord(
    t,
    p,
    o,
    Some(k),
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
                                     padTimestampsMillis: Range = Range.inclusive(1, 10)
  ): Gen[List[ReducedConsumerRecord]] = for {
    t                           <- Gen.const(topic)
    numberOfTotalReducedRecords <- Gen.chooseNum[Int](min, max)
    keysUpper = 2 max (numberOfTotalReducedRecords / 1000)
    numberOfKeys <- Gen.chooseNum[Int](2, keysUpper)
    // Make the number of chars in the key proportional to how many we need to generate
    numberOfCharsInKey = Math.ceil(numberOfKeys.toDouble / 52).toInt
    valueSize <- Gen.choose(1, numberOfCharsInKey)
    keys <- Gen.containerOfN[Set, String](
              numberOfKeys,
              Gen.stringOfN(valueSize, Gen.alphaChar).map(string => Base64.getEncoder.encodeToString(string.getBytes))
            )
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
                                 Gen.chooseNum[Long](padTimestampsMillis.start, padTimestampsMillis.last)
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

  final case class KafkaDataWithTimePeriod(data: List[ReducedConsumerRecord], periodSlice: FiniteDuration) {
    def topics: Set[String] = data.map(_.topic).toSet
    def toKafkaDataWithTimePeriodAndPickedRecord(
        picked: ReducedConsumerRecord
    ): KafkaDataWithTimePeriodAndPickedRecord = KafkaDataWithTimePeriodAndPickedRecord(data, periodSlice, picked)
  }

  final case class KafkaDataWithTimePeriodAndPickedRecord(data: List[ReducedConsumerRecord],
                                                          periodSlice: FiniteDuration,
                                                          picked: ReducedConsumerRecord
  ) {
    def topics: Set[String] = data.map(_.topic).toSet
  }

  /** Generates a random `FiniteDuration` that is between the time periods within the `reducedConsumerRecords`.
    */
  def randomPeriodSliceBetweenMinMax(reducedConsumerRecords: List[ReducedConsumerRecord]): Gen[FiniteDuration] = {
    val head = reducedConsumerRecords.head
    val last = reducedConsumerRecords.last
    Gen.choose[Long](head.timestamp, last.timestamp - 1).map(millis => FiniteDuration(millis, MILLISECONDS))
  }

  def kafkaDateGen(min: Int = 2,
                   max: Int = 100,
                   padTimestampsMillis: Range = Range.inclusive(1, 10),
                   condition: Option[List[ReducedConsumerRecord] => Boolean] = None
  ): Gen[List[ReducedConsumerRecord]] = for {
    topic <- kafkaTopic
    records <- {
      val base = Generators.kafkaReducedConsumerRecordsGen(topic, min, max, padTimestampsMillis)
      condition.fold(base)(c => Gen.listOfFillCond(c, base))
    }
  } yield records

  /** Creates a generated dataset of Kafka events along with a time slice period using sensible values
    * @param min
    *   The minimum number of `ReducedConsumerRecord`'s to generate. Defaults to 2.
    * @param max
    *   The maximum number of `ReducedConsumerRecord`'s to generate. Defaults to 100.
    * @param periodSliceFunction
    *   A provided function that allows you to specify a generated `FiniteDuration` which is handy for
    *   `TimeConfiguration` configuration. The function has both the generated records and `trailingSentinelValue` as an
    *   argument giving you the necessary input to generate the `FiniteDuration` as you wish. Note that if
    *   `trailingSentinelValue` is true then the sentinel value won't be provided to this function.
    * @param padTimestampsMillis
    *   The amount of padding (in milliseconds) specified as a [[scala.collection.immutable.Range]] between consecutive
    *   timestamps. If set to 0 then all timestamps will differ by a single millisecond. Defaults to 10 millis.
    * @param trailingSentinelValue
    *   Whether to add a value at the end of the ReducedConsumerRecords that will have a massively large timestamp so
    *   that it will typically be skipped when backing up data
    */
  def kafkaDataWithTimePeriodsGen(min: Int = 2,
                                  max: Int = 100,
                                  padTimestampsMillis: Range = Range.inclusive(1, 10),
                                  periodSliceFunction: List[ReducedConsumerRecord] => Gen[FiniteDuration] =
                                    randomPeriodSliceBetweenMinMax,
                                  condition: Option[List[ReducedConsumerRecord] => Boolean] = None,
                                  trailingSentinelValue: Boolean = false
  ): Gen[KafkaDataWithTimePeriod] = for {
    records  <- kafkaDateGen(min, max, padTimestampsMillis, condition)
    duration <- periodSliceFunction(records)
  } yield {
    val finalRecords = if (trailingSentinelValue) {
      val last = records.last
      records ::: List(
        ReducedConsumerRecord(last.topic,
                              last.partition,
                              last.offset + 1,
                              None,
                              Base64.getEncoder.encodeToString(Array.empty),
                              System.currentTimeMillis(),
                              TimestampType.CREATE_TIME
        )
      )
    } else records
    KafkaDataWithTimePeriod(finalRecords, duration)
  }

  def kafkaDataWithTimePeriodsAndPickedRecordGen(
      min: Int = 2,
      max: Int = 100,
      padTimestampsMillis: Range = Range.inclusive(1, 10),
      periodSliceFunction: List[ReducedConsumerRecord] => Gen[FiniteDuration] = randomPeriodSliceBetweenMinMax,
      condition: Option[List[ReducedConsumerRecord] => Boolean] = None
  ): Gen[KafkaDataWithTimePeriodAndPickedRecord] = for {
    records           <- kafkaDataWithTimePeriodsGen(min, max, padTimestampsMillis, periodSliceFunction, condition)
    randomRecordIndex <- Gen.choose(0, records.data.length - 1)
  } yield records.toKafkaDataWithTimePeriodAndPickedRecord(records.data(randomRecordIndex))

  def reducedConsumerRecordsUntilSize(size: Long, toBytesFunc: List[ReducedConsumerRecord] => Array[Byte])(
      reducedConsumerRecords: List[ReducedConsumerRecord]
  ): Boolean =
    toBytesFunc(reducedConsumerRecords).length > size

  def timePeriodAlwaysGreaterThanAllMessages(reducedConsumerRecords: List[ReducedConsumerRecord]): Gen[FiniteDuration] =
    FiniteDuration(reducedConsumerRecords.last.timestamp + 1, MILLISECONDS)

  final case class KafkaDataInChunksWithTimePeriod(data: List[List[ReducedConsumerRecord]],
                                                   periodSlice: FiniteDuration
  ) {
    def topics: Set[String] = data.flatten.map(_.topic).toSet
  }

  final case class KafkaDataInChunksWithTimePeriodRenamedTopics(data: List[List[ReducedConsumerRecord]],
                                                                periodSlice: FiniteDuration,
                                                                renamedTopics: Map[String, String]
  ) {
    def topics: Set[String] = data.flatten.map(_.topic).toSet
  }

  /** @param size
    *   The minimum number of bytes
    * @return
    *   A list of [[ReducedConsumerRecord]] that is at least as big as `size`.
    */
  def kafkaDataWithMinByteSizeGen(size: Long,
                                  amount: Int,
                                  toBytesFunc: List[ReducedConsumerRecord] => Array[Byte]
  ): Gen[KafkaDataInChunksWithTimePeriod] = {
    val single =
      kafkaDateGen(1000, 10000, Range.inclusive(1, 10), Some(reducedConsumerRecordsUntilSize(size, toBytesFunc)))
    for {
      recordsSplitBySize <- Gen.sequence(List.fill(amount)(single)).map(_.asScala.toList)
      duration           <- timePeriodAlwaysGreaterThanAllMessages(recordsSplitBySize.flatten)
    } yield KafkaDataInChunksWithTimePeriod(recordsSplitBySize, duration)
  }

  /** @param size
    *   The minimum number of bytes
    * @return
    *   A list of [[ReducedConsumerRecord]] that is at least as big as `size`.
    */
  def kafkaDataWithMinSizeRenamedTopicsGen(size: Long,
                                           amount: Int,
                                           toBytesFunc: List[ReducedConsumerRecord] => Array[Byte]
  ): Gen[KafkaDataInChunksWithTimePeriodRenamedTopics] =
    for {
      kafkaData <- kafkaDataWithMinByteSizeGen(size, amount, toBytesFunc)
      // New topics need to be unique, hence the set
      newTopicNames <-
        Gen.containerOfN[Set, String](kafkaData.topics.size, kafkaTopic.filterNot(kafkaData.topics.contains))
      newTopics = kafkaData.topics.zip(newTopicNames).toMap
    } yield KafkaDataInChunksWithTimePeriodRenamedTopics(kafkaData.data, kafkaData.periodSlice, newTopics)

  private lazy val kafkaTopicBaseGen = Gen.oneOf(Gen.alphaChar, Gen.numChar)
  private lazy val kafkaTopicAllGen =
    Gen.oneOf(Gen.alphaChar, Gen.numChar, Gen.const('-'), Gen.const('.'), Gen.const('_'))

  /** Generator for a valid Kafka topic that can be used in actual Kafka clusters
    */
  lazy val kafkaTopic: Gen[String] = for {
    size <- Gen.choose(1, 249)
    topic <- size match {
               case 1 => kafkaTopicBaseGen.map(_.toString)
               case 2 =>
                 for {
                   first  <- kafkaTopicBaseGen
                   second <- kafkaTopicAllGen
                   order  <- Gen.choose(0, 1) // There isn't a boolean Gen in scalacheck
                 } yield
                   if (order == 0)
                     List(first, second).mkString
                   else
                     List(second, first).mkString
               case _ =>
                 Gen
                   .listOfN(size, kafkaTopicAllGen)
                   .map(_.mkString)
             }
  } yield topic

  /** Generator for a valid Kafka consumer group that can be used in actual Kafka clusters
    */
  lazy val kafkaConsumerGroupGen: Gen[String] = for {
    size    <- Gen.choose(1, 50)
    groupId <- Gen.listOfN(size, Gen.alphaChar)
  } yield groupId.mkString

}
