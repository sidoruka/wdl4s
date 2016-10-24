package wdl4s.values

import wdl4s.parser.WdlParser._
import wdl4s.types._
import wdl4s.util.TryUtil
import scala.collection.JavaConverters._

import scala.util.{Failure, Success, Try}

object WdlValueImplicits {

  // TODO: Needed?
  sealed trait WdlValueMaker[T] {
    def make(ast: AstNode): Try[WdlValue[T]]
  }

  implicit val wdlStringMaker: WdlValueMaker[WdlValue[WdlStringType.type]] = {
    case t: Terminal => Success(WdlString(t.getSourceString))
    case ast => Failure(new SyntaxError(s"Could not convert AST into a WDL String: $ast"))
  }

  implicit val wdlFileMaker: AstNode => Try[WdlValue[WdlFileType.type]] = {
    case t: Terminal => Success(WdlFile(t.getSourceString))
    case ast => Failure(new SyntaxError(s"Could not convert AST into a WDL File: $ast"))
  }

  implicit val wdlIntegerMaker: AstNode => Try[WdlValue[WdlIntegerType.type]] = {
    case t: Terminal => Try(WdlInteger(t.getSourceString.toInt))
    case ast => Failure(new SyntaxError(s"Could not convert AST into a WDL Integer: $ast"))
  }

  implicit val wdlFloatMaker: AstNode => Try[WdlValue[WdlFloatType.type]] = {
    case t: Terminal => Try(WdlFloat(t.getSourceString.toDouble))
    case ast => Failure(new SyntaxError(s"Could not convert AST into a WDL Float: $ast"))
  }

  implicit val wdlBooleanMaker: AstNode => Try[WdlValue[WdlBooleanType.type]] = {
    case t: Terminal if t.getSourceString.toLowerCase.equals("true") => Success(WdlBoolean(true))
    case t: Terminal if t.getSourceString.toLowerCase.equals("false") => Success(WdlBoolean(false))
    case ast => Failure(new SyntaxError(s"Could not convert AST into a WDL Boolean: $ast"))
  }

  implicit def wdlArrayMaker[M]: AstNode => Try[WdlValue[WdlArrayType[M]]] = (a: AstNode) => {
    import wdl4s.AstTools.EnhancedAstNode
     a match {
      case ast: Ast =>
        val elementNodes = ast.getAttribute("values").astListAsVector
        val innerElementMaker = implicitly[AstNode => Try[WdlValue[M]]]
        val elementsTry = TryUtil.sequence(elementNodes map { node => innerElementMaker.apply(node) })
        elementsTry map { elements =>  WdlArray(elements) }
      case _ => Failure(new SyntaxError(s"Could not convert AST into a WDL Array: $a"))
    }
  }

  implicit def wdlMapMaker[K, V]: AstNode => Try[WdlValue[WdlMapType[K, V]]] = (a: AstNode) => {
    a match {
      case ast: Ast =>
        val elements = ast.getAttribute("map").asInstanceOf[AstList].asScala.toVector
        val keyMaker = implicitly[AstNode => Try[WdlValue[K]]]
        val valueMaker = implicitly[AstNode => Try[WdlValue[V]]]
        val kvSeq = TryUtil.sequence(for {
          kvnode <- elements
          kv = for {
            k <- keyMaker.apply(kvnode.asInstanceOf[Ast].getAttribute("key"))
            v <- valueMaker.apply(kvnode.asInstanceOf[Ast].getAttribute("value"))
          } yield k -> v
        } yield kv)

        kvSeq map { kvs => WdlMap(kvs.toMap) }
      case _ => Failure(new SyntaxError(s"Could not convert AST into a WDL Array: $a"))
    }
  }

  implicit def wdlObjectMaker: AstNode => Try[WdlValue[WdlObjectType.type]] = (a: AstNode) => {
    import wdl4s.AstTools.EnhancedAstNode
    a match {
      case ast: Ast =>
        val elements = ast.getAttribute("map").asInstanceOf[AstList].asScala.toVector
        val valueMaker = implicitly[AstNode => Try[WdlValue[String]]]
        val kvSeq = TryUtil.sequence(for {
          kvnode <- elements
          kv = for {
            v <- valueMaker.apply(kvnode.asInstanceOf[Ast].getAttribute("value"))
            k = kvnode.asInstanceOf[Ast].getAttribute("key").sourceString
          } yield k -> v
        } yield kv)

        kvSeq map { kvs => WdlObject(kvs.toMap) }
      case _ => Failure(new SyntaxError(s"Could not convert AST into a WDL Array: $a"))
    }
  }

}
