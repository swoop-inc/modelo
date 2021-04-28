package swoop.modelo

case class ModeloValidationException(smth: String) extends Exception(smth)

object ParamValidators {

  def requireAtLeastOne(params: Map[String, List[String]], required: Set[String]): Unit = {
    if (required.nonEmpty && required.intersect(params.keys.toSet).isEmpty)
      throw ModeloValidationException(s"You supplied these params [${params.keys}] but at least one of these are required [${required}]")
  }

  def requireParams(params: Map[String, List[String]], required: Set[String]): Unit = {
    if (!required.subsetOf(params.keys.toSet))
      throw ModeloValidationException(s"You supplied these params [${params.keys}] but all these are required [${required}]")
  }

}
