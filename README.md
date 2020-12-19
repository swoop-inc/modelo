# modelo

Modelo makes it easy to run Spark queries powered by templates.  "Modelo" means template in Spanish.

It's typically best to write Spark logic via custom transformations with the Scala API.

Sometimes it's easier to express complex logic with pure SQL.  Mustache templates are more powerful than raw SQL strings because you can pass parameters to a template.

`"select * from my_table where first_name = {{{firstName}}}"` is an example of a Mustache template.  `firstName` is a parameter that can be dynamically passed to the template. 

Templates are also great for business users that are comfortable writing pure SQL, but don't want to use the Scala API.  Templates let these users update queries by modifying parameter maps instead of directly modifying SQL strings.

## Simple example

Let's create a DataFrame to demonstrate how to use this lib:

```scala
val df = Seq(
  ("hans", 34),
  ("frank", 15),
  ("luisa", 27),
  ("federica", 78)
).toDF("first_name", "age")

df.createOrReplaceTempView("some_people")
```

Let's run a query on the `some_people` view.

```scala
import mrpowers.modelo.mustache

val template = "select * from {{{viewName}}} where age > {{{minAge}}}"
val attrs = Map("viewName" -> "some_people", "minAge" -> 30)
val res = spark.sql(mustache(template, attrs))
```

This returns the following DataFrame:

```
res.show()

+----------+---+
|first_name|age|
+----------+---+
|      hans| 34|
|  federica| 78|
+----------+---+
```

## Databricks notebook example

Add some pretty pictures with a complicated query to show the power of this design pattern

## Leveraging a partial

ADD description on this powerful paradigm

## Running a template in a transformation chain

Let's see how to run a Mustache template in a chain of DataFrame transformations.

Let's create some fake data with information on dogs.

```scala
import spark.implicits._
val dogs = Seq(
  ("lucky", 2),
  ("tio", 1),
  ("spot", 5),
  ("luna", 13)
).toDF("first_name", "age")
```

Use the Scala API to add an `is_adult` column to the DataFrame.  Then use a Mustache template to filter out all the rows when `is_adult` is false.

```scala
val template = "select * from {{{viewName}}} where is_adult = {{{isAdult}}}"
val attrs = Map("viewName" -> "some_dogs", "isAdult" -> true)
val res = dogs
  // normal DataFrame transformation
  .withColumn("is_adult", $"age" > 3)
  // Running SQL defined in a Mustache view
  .transform(modeloCreateView("some_dogs"))
  .transform(mustacheTransform(template, attrs))
```

Let's see the results:

```
res.show()

+----------+---+--------+
|first_name|age|is_adult|
+----------+---+--------+
|      spot|  5|    true|
|      luna| 13|    true|
+----------+---+--------+
```

This is a simplistic example, so a Mustache template wouldn't really be needed in this case.  Templates are more applicable for complex logic or for users that don't want to interact with the Scala API.

