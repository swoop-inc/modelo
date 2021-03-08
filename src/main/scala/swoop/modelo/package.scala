package swoop

import org.fusesource.scalate.TemplateEngine
import org.fusesource.scalate.util.{FileResourceLoader, Resource}
import org.apache.spark.sql.{DataFrame, SparkSession}

package object modelo {

  // when dealing with templates stored in files, we don't need to worry about "registering templates"
  // crazy template registration hacks are only needed when dealing with template strings
  def mustache(templatePath: os.Path, attrs: Map[String, Any]): String = {
    val engine = new TemplateEngine
    engine.layout(templatePath.toString(), attrs)
  }

  // all templates need to be registered
  // this automatically registers a template for you with a fake name
  // the concept of "registering a template" should be completely unnecessary here
  // Scalate should provide a method like render(template, attrs, "mustache")
  def mustache(template: String, attrs: Map[String, Any], templateName: String = "someFakeTemplateName.mustache"): String = {
    mustache(Map(templateName -> template), templateName, attrs)
  }

  // You need to register multiple templates if you'd like to render partials
  def mustache(templates: Map[String, String], baseTemplateName: String, attrs: Map[String, Any]): String = {
    // this is a hack to get Scalate to work with a custom template: https://scalate.github.io/scalate/documentation/scalate-embedding-guide.html#custom_template_loading
    val engine = new TemplateEngine
    engine.resourceLoader = new FileResourceLoader {
      override def resource(uri: String): Option[Resource] = {
        Some(Resource.fromText(uri, templates(uri)))
      }
    }
    engine.layout(baseTemplateName, attrs)
  }

  def mustacheTransform(template: String, attrs: Map[String, Any])(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    spark.sql(mustache(template, attrs))
  }

  def modeloCreateView(viewName: String)(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView(viewName)
    df
  }

}
