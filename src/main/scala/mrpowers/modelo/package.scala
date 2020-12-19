package mrpowers

import org.fusesource.scalate._
import org.fusesource.scalate.util.{FileResourceLoader, Resource}
import org.apache.spark.sql.{DataFrame, SparkSession}

package object modelo {

  def mustache(templatePath: os.Path, attrs: Map[String, Any]): String = {
    val engine = new TemplateEngine
    engine.layout(templatePath.toString(), attrs)
  }

  def mustache(template: String, attrs: Map[String, Any]): String = {
    // this is a ridiculous hack to get Scalate to work with a custom template: https://scalate.github.io/scalate/documentation/scalate-embedding-guide.html#custom_template_loading
    // the hack is only partially explained in the docs
    // also, why don't the lib give a real solution instead of a hack?
    val engine = new TemplateEngine
    // the mustache extension is important here or else Scalate errors out
    val templates = Map("someFakeTemplateName.mustache" -> template)
    engine.resourceLoader = new FileResourceLoader {
      override def resource(uri: String): Option[Resource] =
        Some(Resource.fromText(uri, templates(uri)))
    }
    engine.layout("someFakeTemplateName.mustache", attrs)
  }

  def mustacheTransform(template: String, attrs: Map[String, Any])(df: DataFrame): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    spark.sql(mustache(template, attrs))
  }

  def modeloCreateView(viewName: String)(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView(viewName)
    df
  }

}
