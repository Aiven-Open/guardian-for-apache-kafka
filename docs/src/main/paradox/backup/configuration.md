# Configuration

## Reference

@@snip (/core-backup/src/main/resources/reference.conf)

Scala API doc @apidoc[kafka.backup.configs.Backup]

## Explanation

* `pekko.kafka.consumer`: See @extref:[documentation](pekko-connectors-kafka-docs:consumer.html#settings)
* `pekko.kafka.consumer.kafka-clients`: See @extref:[documentation](kafka-docs:documentation.html#consumerconfigs)
* `backup`:
    * `kafka-group-id`: The group id for the Kafka consumer that's used in restore tool
    * `time-configuration`: How to slice the persisted keys/files based by time
        * `type`: The type of time configuration. Either `period-from-first` or `chrono-unit-slice`
            * `period-from-first`: Guardian will split up the backup keys/files determined by the `duration` specified.
              The key/filename will be determined by the timestamp of the first message received from the Kafka consumer
              with each further key/filename being incremented by the configured `duration`. If guardian is shut down
              then it will terminate and complete stream with the final element in the JSON array being a `null`
                * This is done so it's possible to determine if a backup has been terminated by shut down of Guardian
                  and also because it's not really possible to resume using arbitrary durations.
            * `chrono-unit-slice`: Guardian will split up the backup keys/files determined by the `chrono-unit` which
              represent intervals such as days and weeks. As such when using this setting its possible for Guardian to
              resume from a previous uncompleted backup.
        * `duration`: If configuration is `period-from-first` then this determines max period of time for each time
          slice.
        * `chrono-unit`: if configuration is `chrono-unit-slice` the `chrono-unit` determines
    * `commit-timeout-buffer-window`: Guardian sets the commit timeout of the Kafka consumer based on the `time-configuration`
      since Guardian does manual committing of cursors. The buffer gets added onto the `time-configuration` to give
      some headroom for any theoretical delays.
    * `compression`: The compression format to use for the data being backed up. Note that changes in compression
      configuration will not apply for any currently existing backups that need to be completed, only for future
      new backups.
      * `type`: Which compression to use.
        * `gzip`. Standard [Gzip](https://en.wikipedia.org/wiki/Gzip) compression
      * `level`: The level of compression to use
