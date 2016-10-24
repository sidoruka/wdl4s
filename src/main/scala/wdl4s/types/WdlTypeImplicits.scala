package wdl4s.types

object WdlTypeImplicits {
  implicit val wdlBooleanType: WdlType[WdlBooleanType.type] = WdlBooleanType
  implicit val wdlFileType: WdlType[WdlFileType.type] = WdlFileType
  implicit val wdlFloatType: WdlFloatType.type = WdlFloatType
  implicit val wdlIntegerType: WdlIntegerType.type = WdlIntegerType
  implicit val wdlStringType: WdlStringType.type = WdlStringType

  implicit def wdlArrayType[T] = WdlArrayType(implicitly[WdlType[T]])
  implicit def wdlMapType[K, V] = WdlMapType(implicitly[WdlType[K]], implicitly[WdlType[V]])
  implicit def wdlPairType[L, R] = WdlPairType(implicitly[WdlType[L]], implicitly[WdlType[R]])
}
