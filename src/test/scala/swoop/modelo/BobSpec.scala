package swoop.modelo

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.{FunSpec, Matchers}

class BobSpec extends FunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  it("dynamically builds input parameters") {
    val someTool = Bob(
      templates = Map("countryFiltered" -> "select * from my_table where country IN {{{countries}}}"),
      baseTemplateName = "countryFiltered",
      required = Set("whatever", "cool"),
      paramConverters = Map("whatever" -> ParamConverters.multiMatch, "cool" -> ParamConverters.exactMatch)
    )
    val b = someTool
      .whatever("aaa", "bbb")
      .cool("ccc")
    val expected = Map("whatever" -> "('aaa','bbb')", "cool" -> "'ccc'")
    b.attributes should be(expected)
  }

  it("can dynamically run queries") {
    val df = Seq(
      ("li", "china"),
      ("luis", "colombia"),
      ("fernanda", "brasil")
    ).toDF("first_name", "country")
    df.createOrReplaceTempView("my_table")
    // technical users construct someTool
    val someTool = Bob(
      templates = Map("countryFiltered.mustache" -> "select * from my_table where country IN {{{countries}}}"),
      baseTemplateName = "countryFiltered.mustache",
      required = Set("countries"),
      paramConverters = Map("countries" -> ParamConverters.multiMatch)
    )
    // less technical users run queries with this interface
    val b = someTool.countries("china", "colombia")
    val expected = Seq(
      ("li", "china"),
      ("luis", "colombia")
    ).toDF("first_name", "country")
    assertSmallDataFrameEquality(b.dataframe, expected, orderedComparison = false)
  }

  it("errors out if a required param isn't supplied") {
    val someTool = Bob(
      templates = Map("countryFiltered.mustache" -> "select * from my_table where country IN {{{countries}}}"),
      baseTemplateName = "countryFiltered.mustache",
      required = Set("countries", "cat"),
      paramConverters = Map("countries" -> ParamConverters.multiMatch, "cat" -> ParamConverters.exactMatch)
    )
    val b = someTool
      .cool("ccc")
    intercept[ModeloValidationException] {
      b.attributes
    }
  }

  it("errors out if none of the required params are supplied") {
    val someTool = Bob(
      templates = Map("countryFiltered.mustache" -> "select * from my_table where country IN {{{countries}}}"),
      baseTemplateName = "countryFiltered.mustache",
      required = Set("countries"),
      paramConverters = Map("countries" -> ParamConverters.multiMatch)
    )
    val b = someTool
      .whatever("aaa", "bbb")
      .cool("ccc")
    intercept[ModeloValidationException] {
      b.attributes
    }
  }

}
