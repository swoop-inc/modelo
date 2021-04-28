package swoop.modelo

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.{FunSpec, Matchers}

class ModeloSpec extends FunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer {

  describe("mustache") {

    it("generates a string from a template file") {
      val path = os.pwd / "src" / "test" / "resources" / "template1.mustache"
      val expected = "select * from cool_view where age > 30"
      mustache(path, Map("viewName" -> "cool_view", "minAge" -> 30)) should be(expected)
    }

    it("generates a string from a template string") {
      val template = "select * from {{{viewName}}} where age > {{{minAge}}}"
      val expected = "select * from cool_view where age > 30"
      mustache(template, Map("viewName" -> "cool_view", "minAge" -> 30)) should be(expected)
    }

    it("can render a template with a partial for files") {
      val path = os.pwd / "src" / "test" / "resources" / "base.mustache"
      val res = mustache(path, Map("names" -> List(Map("name" -> "Marcela"), Map("name" -> "Luisa"))))
      val expected =
        """<h2>Names</h2>
          |<strong>Marcela</strong>
          |<strong>Luisa</strong>
          |""".stripMargin
      res should be(expected)
    }

    it("can render a template with a partial for strings via template registration") {
      val templateBase = """<h2>Names</h2>
                           |{{#names}}
                           |  {{> user}}
                           |{{/names}}
                           |""".stripMargin
      val templatePartial = "<strong>{{name}}</strong>"
      val templates = Map("base.mustache" -> templateBase, "user.mustache" -> templatePartial)
      val attrs = Map("names" -> List(Map("name" -> "Marcela"), Map("name" -> "Luisa")))
      val res = mustache(templates, "base.mustache", attrs)
      val expected =
        """<h2>Names</h2>
          |<strong>Marcela</strong>
          |<strong>Luisa</strong>
          |""".stripMargin
      res should be(expected)
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

    it("shows how to use a partial in a Spark related query") {
      import spark.implicits._
      val df = Seq(
        ("crosby", 1987, true),
        ("gretzky", 1961, false),
        ("jagr", 1972, false),
        ("mcdavid", 1997, true)
      ).toDF("player", "birth_year", "is_active")
      df.createOrReplaceTempView("hockey_players")
      val ageCalculator = "year(current_date()) - birth_year as age"
      val baseTemplate = "select {{> ageCalculator}} from {{{viewName}}} where is_active = {{{isActive}}}"
      val templates = Map("baseTemplate.mustache" -> baseTemplate, "ageCalculator.mustache" -> ageCalculator)
      val attrs = Map("viewName" -> "hockey_players", "isActive" -> true)
      val res = spark.sql(mustache(templates, "baseTemplate.mustache", attrs))
      // intentionally not putting an assertion here cause it'll break when the year changes
      res.show()
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
