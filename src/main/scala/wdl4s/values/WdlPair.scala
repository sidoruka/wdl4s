package wdl4s.values

import wdl4s.types.{WdlPairType, WdlType}

case class WdlPair[L <: WdlType[L], R <: WdlType[R]](left: WdlValue[L], right: WdlValue[R]) extends WdlValue[WdlPairType[L, R]] {
  override val wdlType: WdlType = WdlPairType(left.wdlType, right.wdlType)
}
