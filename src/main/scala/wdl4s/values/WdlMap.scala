package wdl4s.values

import wdl4s.TsvSerializable
import wdl4s.types.{WdlMapType, WdlType}
import wdl4s.util.{FileUtil, TryUtil}

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object WdlMap {
  def coerceMap[A, B, K, V](m: Map[A, B], wdlMapType: WdlMapType[K, V]): WdlMap[K, V] = {
    val coerced = m map { case(k, v) => wdlMapType.keyType.coerceRawValue(k) -> wdlMapType.valueType.coerceRawValue(v) }
    val failures = coerced flatMap { case(k,v) => Seq(k,v) } collect { case f:Failure[_] => f }
    failures match {
      case f: Iterable[Failure[_]] if f.nonEmpty =>
        throw new UnsupportedOperationException(s"Failed to coerce one or more keys or values for creating a ${wdlMapType.toWdlString}:\n${TryUtil.stringifyFailures(f)}}")
      case _ =>
        val mapCoerced = coerced map { case (k, v) => k.get -> v.get }
        WdlMap(mapCoerced)
    }
  }

  def fromTsv[K <: WdlType, V <: WdlType](tsv: String, wdlMapType: WdlMapType[K, V]): Try[WdlMap[K, V]] = {
    FileUtil.parseTsv(tsv) match {
      case Success(table) if table.isEmpty => Success(WdlMap(Map.empty[WdlValue[K], WdlValue[V]]))
      case Success(table) if table.head.length != 2 => Failure(new UnsupportedOperationException("TSV must be 2 columns to convert to a Map"))
      case Success(table) => Try(coerceMap(table.map(row => row(0) -> row(1)).toMap, wdlMapType))
      case Failure(e) => Failure(e)
    }
  }
}

case class WdlMap[K <: WdlType, V <: WdlType](value: Map[WdlValue[K], WdlValue[V]]) extends WdlValue[WdlMapType[K, V]] with TsvSerializable {
  import wdl4s.types.WdlTypeImplicits._
  val wdlType = implicitly[WdlMapType[K, V]]

  override def toWdlString: String =
    "{" + value.map {case (k,v) => s"${k.toWdlString}: ${v.toWdlString}"}.mkString(", ") + "}"

  def tsvSerialize: Try[String] = {
    if (this.isInstanceOf[WdlMap[_ <: WdlPrimitive, _ <: WdlPrimitive]]) {
      Success(value.map({ case (k, v) => s"${k.valueString}\t${v.valueString}" }).mkString("\n"))
    } else {
      Failure(new UnsupportedOperationException("Can only TSV serialize a Map[Primitive, Primitive]"))
    }
  }

  def map[K2, V2](f: PartialFunction[((WdlValue[K], WdlValue[V])), (WdlValue[K2], WdlValue[V2])]): WdlMap[K2, V2] = {
    WdlMap[K2, V2](value map f)
  }

  override def collectAsSeq[T <: WdlValue](filterFn: PartialFunction[WdlValue[_], T]): Seq[T] = {
    val collected = value flatMap {
      case (k, v) => Seq(k.collectAsSeq(filterFn), v.collectAsSeq(filterFn))
    }
    collected.flatten.toSeq
  }
}
