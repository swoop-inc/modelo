# modelo

Modelo makes it easy to run Spark queries powered by templates.

```scala
import mrpowers.modelo.mustache

val template = "select * from {{{viewName}}} where age > {{{minAge}}}"
val attrs = Map("viewName" -> "some_people", "minAge" -> 30)
val newDF = df
  .transform(mustache(template, attrs))
```
