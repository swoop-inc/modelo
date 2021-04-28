package swoop.modelo

object ParamConverters {

  def multiMatch(strs: List[String]): String = {
    strs.map(str => s"'${str}'").mkString("(", ",", ")")
  }

  def exactMatch(strs: List[String]): String = {
    "'" + strs(0) + "'"
  }

}
