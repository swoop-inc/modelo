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
        if (key.endsWith("Exact")) {
          (key, ParamConverters.exactMatch(value))
        } else if (key.endsWith("Exacts")) {
          (key, ParamConverters.multiMatch(value))
        } else if (paramConverters.contains(key)) {
          (key, paramConverters(key)(value))
        } else {
          (key, ParamConverters.exactMatch(value))
        }
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
