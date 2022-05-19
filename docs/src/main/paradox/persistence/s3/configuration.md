# S3

## Reference

@@snip (/core-s3/src/main/resources/reference.conf)

Scala API doc @apidoc[kafka.s3.configs.S3]

## Explanation

* `s3-headers`: See @extref:[documentation](alpakka:akka/stream/alpakka/s3/headers/index.html)
* `alpakka.s3`: See @extref:[documentation](alpakka-docs:s3.html#configuration)
* `s3-config`: Core S3 configuration
    * `data-bucket`: The main S3 bucket where data is backed up and where to restore data from
    * `data-bucket-prefix`: S3 prefix configuration to be used when searching for the bucket
    * `error-restart-settings`: Specific retry settings when recovering from known errors in S3 
