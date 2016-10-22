package wdl4s.types

import wdl4s.values.{WdlArray, WdlFile, WdlString, WdlValue}
import wdl4s.util.TryUtil
import spray.json.JsArray

import scala.util.{Failure, Success}

case class WdlArrayType[M <: WdlType](memberType: WdlType[M]) extends WdlType[WdlArrayType[M]] {
  val toWdlString: String = s"Array[${memberType.toWdlString}]"

  private def coerceIterable(values: Seq[Any]): WdlArray[M] = values match {
    case s:Seq[Any] if s.nonEmpty =>
      val coerced = s.map {memberType.coerceRawValue(_).get}
      WdlArray(coerced)
    case _ => WdlArray(Seq.empty[WdlValue[M]])
  }

  override protected def coercion = {
    case s: Seq[Any] => coerceIterable(s)
    case js: JsArray => coerceIterable(js.elements)
    case wdlArray: WdlArray =>
      val arrayType: WdlArrayType[_] = wdlArray.wdlType

    case wdlArray: WdlArray if wdlArray.wdlType.memberType == WdlStringType && memberType == WdlFileType =>
      WdlArray(wdlArray.value.map(str => WdlFile(str.asInstanceOf[WdlString].value)).toList)
    case wdlArray: WdlArray if wdlArray.wdlType.memberType == memberType => wdlArray
    case wdlArray: WdlArray if wdlArray.wdlType.memberType == WdlAnyType => coerceIterable(wdlArray.value)
    case wdlArray: WdlArray if wdlArray.wdlType.memberType.isInstanceOf[WdlArrayType] && memberType.isInstanceOf[WdlArrayType] =>
      TryUtil.sequence(wdlArray.value.map(memberType.coerceRawValue)) match {
        case Success(values) => WdlArray(WdlArrayType(memberType), values)
        case Failure(ex) => throw ex
      }
    case wdlArray: WdlArray if memberType.isCoerceableFrom(wdlArray.wdlType.memberType) =>
      wdlArray.map(v => memberType.coerceRawValue(v).get) // .get because isCoerceableFrom should make it safe
    case wdlValue: WdlValue if memberType.isCoerceableFrom(wdlValue.wdlType) =>
      memberType.coerceRawValue(wdlValue) match {
        case Success(coercedValue) => WdlArray(this, Seq(coercedValue))
        case Failure(ex) => throw ex
      }
  }

  override def isCoerceableFrom[V <: WdlType](otherType: WdlType[V]): Boolean = otherType match {
    case a: WdlArrayType => memberType.isCoerceableFrom(a.memberType)
    case _ => false
  }
}

object WdlArrayType {

  implicit class WdlArrayEnhanced[U](wdlType: WdlType[U]) extends WdlType[U] {

    override protected def coercion: PartialFunction[Any, WdlValue] = wdlType.coercion
    override def toWdlString: String = wdlType.toWdlString

    def isAnArrayOf(genericType: WdlType) = wdlType.isInstanceOf[WdlArrayType] && wdlType.asInstanceOf[WdlArrayType].memberType == genericType
  }
}
