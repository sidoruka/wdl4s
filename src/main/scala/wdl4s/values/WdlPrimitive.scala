package wdl4s.values

import wdl4s.types.WdlType

trait WdlPrimitive[T <: WdlType[T]] extends WdlValue[T]