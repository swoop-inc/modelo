package swoop.modelo

import scala.language.dynamics
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Bob(
    templates: Map[String, String],
    baseTemplateName: String,
    inputParamsValidations: List[(Map[String, List[String]]) => Unit],
    inputParams: Map[String, List[String]] = Map.empty[String, List[String]],
    paramConverters: Map[String, List[String] => String] = Map.empty[String, List[String] => String]
) extends Dynamic {

  def applyDynamic(name: String)(args: String*): Bob = {
    copy(inputParams = inputParams ++ Map(name -> args.toList))
  }

  def attributes: Map[String, Any] = {
    inputParamsValidations.foreach { fun: (Map[String, List[String]] => Unit) =>
      fun(inputParams)
    }
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
