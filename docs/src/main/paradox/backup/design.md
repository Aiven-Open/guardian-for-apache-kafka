# Design

The format for backups is in JSON consisting of a large JSON array filled with JSON objects that have the following
format.

```json
{
  "topic": "kafka topic",
  "partition": 0,
  "offset": 0,
  "key": "a2V5",
  "value": "dmFsdWU=",
  "timestamp": 0,
  "timestamp_type": 0
}
```

The `key` and `value` are Base64 encoded byte arrays (in the above example `"a2V5"` decodes to the string `key`
and `"dmFsdWU="` decodes to the string `value`). This is due to the fact that the backup tool can make no assumptions on
the format of the key or value, so we encode the raw byte arrays.

One thing to note is that its possible for the last JSON object in the JSON array to be `null`, see for more info.
