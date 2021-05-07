package swoop.modelo

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.{FunSpec, Matchers}
import ParamValidators._
import ParamConverters._

class BobSpec extends FunSpec with Matchers with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  it("dynamically builds input parameters") {
    val someTool = Bob(
      templates = Map("countryFiltered" -> "select * from my_table where country IN {{{countries}}}"),
      baseTemplateName = "countryFiltered",
      inputParamsValidations = List(requireParams(Set("whateverExacts", "coolExact")))
    )
    val b = someTool
      .whateverExacts("aaa", "bbb")
      .coolExact("ccc")
      .hiExact("hello")
    val expected = Map("whateverExacts" -> "('aaa','bbb')", "coolExact" -> "'ccc'", "hiExact" -> "'hello'")
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
      templates = Map("countryFiltered.mustache" -> "select * from my_table where country IN {{{countryExacts}}}"),
      baseTemplateName = "countryFiltered.mustache",
      inputParamsValidations = List(requireParams(Set("countryExacts")))
    )
    // less technical users run queries with this interface
    val b = someTool.countryExacts("china", "colombia")
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
      inputParamsValidations = List(requireParams(Set("countries", "cat"))),
      paramConverters = Map("countries" -> multiMatch, "cat" -> exactMatch)
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
      inputParamsValidations = List(requireParams(Set("countries"))),
      paramConverters = Map("countries" -> multiMatch)
    )
    val b = someTool
      .whatever("aaa", "bbb")
      .cool("ccc")
    intercept[ModeloValidationException] {
      b.attributes
    }
  }

}
