package wdl4s.values

import wdl4s.TsvSerializable
import wdl4s.types._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object WdlArray {
  def fromTsv(tsv: String): WdlArray[WdlStringType.type] = {
    WdlArray[WdlStringType.type](tsv.replaceAll("[\r\n]+$", "").split("[\n\r]+").toSeq map { line =>
      WdlArray[WdlStringType.type](line.split("\t").toSeq.map(WdlString))
    })
  }
}

case class WdlArray[A <: WdlType](value: Seq[WdlValue[A]]) extends WdlValue[WdlArrayType[A]] with TsvSerializable {
  import wdl4s.types.WdlTypeImplicits._
  val wdlType: WdlArrayType[A] = implicitly[WdlArrayType[A]]

  val typesUsedInValue = Set(value map {_.wdlType}: _*)
  if (typesUsedInValue.size == 1 && typesUsedInValue.head != wdlType.memberType) {
    throw new UnsupportedOperationException(s"Could not construct array of type $wdlType with this value: $value")
  }
  if (typesUsedInValue.size > 1) {
    throw new UnsupportedOperationException(s"Cannot construct array with a mixed types: $value")
  }

  override def toWdlString: String = s"[${value.map(_.toWdlString).mkString(", ")}]"
  override def toString = toWdlString

  def map[V, R <: WdlTypedValue[V]](f: WdlTypedValue[A] => R): WdlArray = {
    value map { f(_) } match {
      case s: Seq[R] if s.nonEmpty => WdlArray(WdlArrayType[R](s.head.wdlType), s)
      case _ => WdlArray[R](WdlArrayType[R], Seq.empty)
    }
  }

  def tsvSerialize: Try[String] = {
    wdlType.memberType match {
      case t: WdlPrimitiveType => Success(value.map(_.valueString).mkString("\n"))
      case WdlObjectType => WdlObject.tsvSerializeArray(value map { _.asInstanceOf[WdlObject] })
      case WdlArrayType(t: WdlPrimitiveType) =>
        val tsvString = value.collect({ case a: WdlArray => a }) map { a =>
          a.value.collect({ case p: WdlPrimitive => p }).map(_.valueString).mkString("\t")
        } mkString "\n"
        Success(tsvString)
      case _ => Failure(new UnsupportedOperationException(s"Cannot TSV serialize a ${this.wdlType.toWdlString} (valid types are Array[Primitive], Array[Array[Primitive]], or Array[Object])"))
    }
  }

  override def collectAsSeq[T <: WdlValue](filterFn: PartialFunction[WdlValue, T]): Seq[T] = {
    value flatMap { _.collectAsSeq(filterFn) }
  }
}
