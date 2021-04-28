package swoop.modelo

import scala.language.dynamics
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Bob(
    templates: Map[String, String],
    baseTemplateName: String,
    inputParams: Map[String, List[String]] = Map.empty[String, List[String]],
    required: Set[String] = Set.empty[String],
    requireAtLeastOne: Set[String] = Set.empty[String],
    paramConverters: Map[String, List[String] => String] = Map.empty[String, List[String] => String]
) extends Dynamic {

  def applyDynamic(name: String)(args: String*): Bob = {
    copy(inputParams = inputParams ++ Map(name -> args.toList))
  }

  def required(value: Set[String]): Bob = copy(required = value)

  def requireAtLeastOne(value: Set[String]): Bob = copy(requireAtLeastOne = value)

  def attributes: Map[String, Any] = {
    ParamValidators.requireParams(inputParams, required)
    ParamValidators.requireAtLeastOne(inputParams, requireAtLeastOne)
    inputParams.map {
      case (key, value) =>
        (key, paramConverters(key)(value))
    }
  }

  def render(): String = {
    mustache(templates, baseTemplateName, attributes)
  }

  def dataframe: DataFrame = {
    val spark = SparkSession.getActiveSession.get
    spark.sql(render())
  }

}
