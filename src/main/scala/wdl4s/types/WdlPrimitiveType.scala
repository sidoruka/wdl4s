package wdl4s.types

import wdl4s.values.{WdlBoolean, WdlFile, WdlFloat, WdlInteger, WdlString}

abstract class WdlPrimitiveType[U <: WdlPrimitiveType] extends WdlType[U] {
  lazy val coercionMap: Map[WdlType[_], Seq[WdlType[_]]] = Map(
    // From type -> To type
    WdlStringType -> Seq(WdlStringType, WdlIntegerType, WdlFloatType, WdlFileType, WdlBooleanType),
    WdlFileType -> Seq(WdlStringType, WdlFileType),
    WdlIntegerType -> Seq(WdlStringType, WdlIntegerType, WdlFloatType),
    WdlFloatType -> Seq(WdlStringType, WdlFloatType),
    WdlBooleanType -> Seq(WdlStringType, WdlBooleanType)
  )

  override def isCoerceableFrom[V <: WdlType](otherType: WdlType[V]): Boolean = {
    coercionMap.get(otherType) match {
      case Some(types) => types contains this
      case None => false
    }
  }
}

object WdlPrimitiveType {

  val primitiveTypes = List(WdlBoolean, WdlFile, WdlFloat, WdlInteger, WdlString)

  def tryMkTypeObject[T <: WdlType[T]]: Option[WdlPrimitiveType[T]] = {
    primitiveTypes.collect {
      case t: T with WdlPrimitiveType[T] => t
    }.headOption
  }

  def mkTypeObject[T <: WdlPrimitiveType[T]]: T with WdlType[T] = {
    (primitiveTypes collect {
      case t: T with WdlType[T] => t
    }).head
  }
}
