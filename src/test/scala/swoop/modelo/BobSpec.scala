package swoop.modelo

import org.scalatest.{FunSpec, Matchers}

class BobSpec extends FunSpec with Matchers {

  it("dynamically builds input parameters") {
    val b = Bob(required = Set("whatever", "cool"))
      .whatever("aaa", "bbb")
      .cool("ccc")
    val expected = Map("whatever" -> List("aaa", "bbb"), "cool" -> List("ccc"))
    b.attributes should be(expected)
  }

  it("errors out if a required param isn't supplied") {
    val b = Bob(required = Set("fun"))
      .whatever("aaa", "bbb")
      .cool("ccc")
    intercept[ModeloBobException] {
      b.attributes
    }
  }

  it("errors out if none of the required params are supplied") {
    val b = Bob(requireAtLeastOne = Set("fun"))
      .whatever("aaa", "bbb")
      .cool("ccc")
    intercept[ModeloBobException] {
      b.attributes
    }
  }

  it("does not error out if at least one of the required params is supplied") {
    val b = Bob(requireAtLeastOne = Set("cool"))
      .whatever("aaa", "bbb")
      .cool("ccc")
    val expected = Map("whatever" -> List("aaa", "bbb"), "cool" -> List("ccc"))
    b.attributes should be(expected)
  }

}
