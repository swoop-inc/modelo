package swoop.modelo

import scala.language.dynamics

case class ModeloBobException(smth: String)  extends Exception(smth)

case class Bob(
    inputParams: Map[String, List[String]] = Map.empty[String, List[String]],
    required: Set[String] = Set.empty[String],
    requireAtLeastOne: Set[String] = Set.empty[String]
  ) extends Dynamic {

  def applyDynamic(name: String)(args: String*): Bob = {
    copy(inputParams = inputParams ++ Map(name -> args.toList))
  }

  def required(value: Set[String]): Bob = copy(required = value)

  def requireAtLeastOne(value: Set[String]): Bob = copy(requireAtLeastOne = value)

  def attributes: Map[String, Any] = {
    if (!required.subsetOf(inputParams.keys.toSet))
      throw ModeloBobException(s"You supplied these params [${inputParams.keys}] but all these are required [${required}]")
    if (requireAtLeastOne.nonEmpty && requireAtLeastOne.intersect(inputParams.keys.toSet).isEmpty)
      throw ModeloBobException(s"You supplied these params [${inputParams.keys}] but at least one of these are required [${requireAtLeastOne}]")
    inputParams
  }

}