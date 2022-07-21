# CI - Continuous Integration

Guardian uses github actions to perform CI whenever a pull request is made and when a pull request is merged into
master. CI is also responsible for publishing github github. The integration with github actions for the main build is
performed using [sbt-github-actions][sbt-github-actions-link].

## Design

One thing to note about [sbt-github-actions][sbt-github-actions-link] is that it generates the github workflow files
directly from the sbt @github[build definition file](/build.sbt).
This means that the `build.sbt` is the source of truth and hence [sbt-github-actions][sbt-github-actions-link] also
checks that the github workflow is in sync with `build.sbt` as part of the CI process.

Essentially that means any changes to `build.sbt` (such as updating Scala versions) can also cause changes in github
workflow actions. Likewise if you need to do any custom changes to
the @github[ci.yaml](/.github/workflows/ci.yml) file you need to do this in `build.sbt` using
[sbt-github-actions][sbt-github-actions-link] SBT dsl.

To regenerate the relevant github workflow files after changes to `build.sbt` are done you need to run

```
githubWorkflowGenerate
```

In the sbt shell. For more information go [here](https://github.com/djspiewak/sbt-github-actions#generative-plugin)

## Scalafmt

In addition and separately to [sbt-github-actions][sbt-github-actions-link] Guardian also has
a [scalafmt][scalafmt-link] pipeline that checks the code is correctly formatted on each PR. This allows the
@github[scalafmt pipeline](/.github/workflows/format.yml) to run at the same time the main build 
does. Furthermore, it uses [scalafmt-native](https://scalameta.org/scalafmt/docs/installation.html#native-image) for
improved runtime performance (typically it takes 5-10 seconds to check the entire project is formatted).

This means that if you ever update the scalafmt version in
the @github[configuration file](/.scalafmt.conf#L1) you also need to update it in the
@github[scalafmt-pipeline](/.github/workflows/format.yml#L26).

[sbt-github-actions-link]: https://github.com/djspiewak/sbt-github-actions
[scalafmt-link]: https://scalameta.org/scalafmt/
