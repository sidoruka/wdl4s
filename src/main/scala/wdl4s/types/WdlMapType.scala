package wdl4s.types

import wdl4s.values._
import spray.json.JsObject

case class WdlMapType[K <: WdlType, V <: WdlType](keyType: WdlType[K], valueType: WdlType[V]) extends WdlType[WdlMapType[K, V]] {
  val toWdlString: String = s"Map[${keyType.toWdlString}, ${valueType.toWdlString}]"

  override protected def coercion = {
    case m: Map[_, _] if m.nonEmpty => WdlMap.coerceMap(m, this)
    case m: Map[_, _] if m.isEmpty => WdlMap(Map.empty[WdlValue[K], WdlValue[V]])
    case js: JsObject if js.fields.nonEmpty => WdlMap.coerceMap(js.fields, this)
    case wdlMap: WdlMap => WdlMap.coerceMap(wdlMap.value, this)
    case o: WdlObject => WdlMap.coerceMap(o.value, this)
  }

  override def isCoerceableFrom[A <: WdlType](otherType: WdlType[A]): Boolean = otherType match {
    case m: WdlMapType => keyType.isCoerceableFrom(m.keyType) && valueType.isCoerceableFrom(m.valueType)
    case WdlObjectType => keyType.isCoerceableFrom(WdlStringType) && valueType.isCoerceableFrom(WdlStringType)
    case _ => false
  }
}
