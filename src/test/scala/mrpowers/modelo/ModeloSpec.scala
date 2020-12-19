package mrpowers.modelo

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.{FunSpec, Matchers}

class ModeloSpec extends FunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer {

  describe("mustache") {

    it("generates a string from a template file") {
      val path = os.pwd/"src"/"test"/"resources"/"template1.mustache"
      val expected = "select * from cool_view where age > 30"
      mustache(path, Map("viewName" -> "cool_view", "minAge" -> 30)) should be(expected)
    }

    it("generates a string from a template string") {
      val template = "select * from {{{viewName}}} where age > {{{minAge}}}"
      val expected = "select * from cool_view where age > 30"
      mustache(template, Map("viewName" -> "cool_view", "minAge" -> 30)) should be(expected)
    }

    it("works seamlessly with Spark") {
      import spark.implicits._
      val df = Seq(
        ("hans", 34),
        ("frank", 15),
        ("luisa", 27),
        ("federica", 78)
      ).toDF("first_name", "age")
      df.createOrReplaceTempView("some_people")
      val template = "select * from {{{viewName}}} where age > {{{minAge}}}"
      val attrs = Map("viewName" -> "some_people", "minAge" -> 30)
      val res = spark.sql(mustache(template, attrs))
      val expected = Seq(
        ("hans", 34),
        ("federica", 78)
      ).toDF("first_name", "age")
      assertSmallDataFrameEquality(res, expected)
    }

    it("demonstrates how to chain with other custom transformations") {
      import spark.implicits._
      val dogs = Seq(
        ("lucky", 2),
        ("tio", 1),
        ("spot", 5),
        ("luna", 13)
      ).toDF("first_name", "age")
      val template = "select * from {{{viewName}}} where is_adult = {{{isAdult}}}"
      val attrs = Map("viewName" -> "some_dogs", "isAdult" -> true)
      val res = dogs
        // normal DataFrame transformation
        .withColumn("is_adult", $"age" > 3)
        // Running SQL defined in a Mustache view
        .transform(modeloCreateView("some_dogs"))
        .transform(mustacheTransform(template, attrs))
      val expected = Seq(
        ("spot", 5, true),
        ("luna", 13, true)
      ).toDF("first_name", "age", "is_adult")
      assertSmallDataFrameEquality(res, expected)
    }

  }

}
