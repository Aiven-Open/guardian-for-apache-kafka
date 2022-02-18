# Security

## OWASP Report

Guardian uses [sbt-dependency-check](https://github.com/albuch/sbt-dependency-check) to generate
a [dependency-check-report][dependency-check-report-link] which checks direct and transitive dependencies for
vulnerabilities against [NVD](https://nvd.nist.gov/) in the form of a HTML file that can be viewed in a standard
browser.

### Generating a report

You can use the sbt shell to generate a report at any time using

```
dependencyCheckAggregate
```

This will overwrite the [current report file][dependency-check-report-link]

### Suppressing false positives

Sometimes it is possible that a false positive get generated in the report. To add a false positive, first you need to
open the [report file][dependency-check-report-link] in a supported browser. In the list of found vulnerabilities there
should be a suppress button which when clicked displays a popup containing an `XML` suppression entry. You then add
that `<suppress>` tag entry to the
existing [suppression-file](https://github.com/aiven/guardian-for-apache-kafka/edit/main/dependency-check/suppression.xml)
. Finally, regenerate the report again using sbt's `dependencyCheckAggregate`

[dependency-check-report-link]: https://github.com/aiven/guardian-for-apache-kafka/blob/main/dependency-check/dependency-check-report.html
