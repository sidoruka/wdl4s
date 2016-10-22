package wdl4s.expression

import wdl4s.types._
import wdl4s.values._
import wdl4s.{TsvSerializable, WdlExpressionException}

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait WdlStandardLibraryFunctions extends WdlFunctions[WdlValue] {
  def fileContentsToString(path: String): String = readFile(path)
  def readFile(path: String): String
  def writeTempFile(path: String, prefix: String, suffix: String, content: String): String
  def stdout(params: Seq[Try[WdlValue]]): Try[WdlFile]
  def stderr(params: Seq[Try[WdlValue]]): Try[WdlFile]
  def glob(path: String, pattern: String): Seq[String]
  def write_tsv(params: Seq[Try[WdlValue]]): Try[WdlFile]
  def write_json(params: Seq[Try[WdlValue]]): Try[WdlFile]
  def size(params: Seq[Try[WdlValue]]): Try[WdlFloat]

  def read_objects(params: Seq[Try[WdlValue]]): Try[WdlArray] = extractObjects(params) map { WdlArray(WdlArrayType(WdlObjectType), _) }
  def read_string(params: Seq[Try[WdlValue]]): Try[WdlString] = readContentsFromSingleFileParameter(params).map(s => WdlString(s.trim))
  def read_json(params: Seq[Try[WdlValue]]): Try[WdlValue]
  def read_int(params: Seq[Try[WdlValue]]): Try[WdlInteger] = read_string(params) map { s => WdlInteger(s.value.trim.toInt) }
  def read_float(params: Seq[Try[WdlValue]]): Try[WdlFloat] = read_string(params) map { s => WdlFloat(s.value.trim.toDouble) }

  def write_lines(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlArray])
  def write_map(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlMap])
  def write_object(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlObject])
  def write_objects(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlArray])

  def read_lines(params: Seq[Try[WdlValue]]): Try[WdlArray] = {
    for {
      contents <- readContentsFromSingleFileParameter(params)
      lines = contents.split("\n")
    } yield WdlArray(WdlArrayType(WdlStringType), lines map WdlString)
  }

  def read_map(params: Seq[Try[WdlValue]]): Try[WdlMap[WdlAnyType.type, WdlAnyType.type]] = {
    for {
      contents <- readContentsFromSingleFileParameter(params)
      wdlMap <- WdlMap.fromTsv(contents, WdlMapType(WdlAnyType, WdlAnyType))
    } yield wdlMap
  }

  def read_object(params: Seq[Try[WdlValue]]): Try[WdlObject] = {
    extractObjects(params) map {
      case array if array.length == 1 => array.head
      case _ => throw new IllegalArgumentException("read_object yields an Object and thus can only read 2-rows TSV files. Try using read_objects instead.")
    }
  }

  def read_tsv(params: Seq[Try[WdlValue]]): Try[WdlArray] = {
    for {
      contents <- readContentsFromSingleFileParameter(params)
      wdlArray = WdlArray.fromTsv(contents)
    } yield wdlArray
  }

  def read_boolean(params: Seq[Try[WdlValue]]): Try[WdlBoolean] = {
    read_string(params) map { s => WdlBoolean(java.lang.Boolean.parseBoolean(s.value.trim.toLowerCase)) }
  }

  def glob(params: Seq[Try[WdlValue]]): Try[WdlArray] = {
    for {
      singleArgument <- extractSingleArgument(params)
      globVal = singleArgument.valueString
      files = glob(globPath(globVal), globVal)
      wdlFiles = files map { WdlFile(_, isGlob = false) }
    } yield WdlArray(WdlArrayType(WdlFileType), wdlFiles)
  }

  def transpose(params: Seq[Try[WdlValue]]): Try[WdlArray] = {
    def extractExactlyOneArg: Try[WdlValue] = params.size match {
      case 1 => params.head
      case n => Failure(new IllegalArgumentException(s"Invalid number of parameters for engine function transpose: $n. Ensure transpose(x: Array[Array[X]]) takes exactly 1 parameters."))
    }

    case class ExpandedTwoDimensionalArray(innerType: WdlType, value: Seq[Seq[WdlValue]])
    def validateAndExpand(value: WdlValue): Try[ExpandedTwoDimensionalArray] = value match {
      case WdlArray(WdlArrayType(WdlArrayType(innerType)), array: Seq[WdlValue]) => expandWdlArray(array) map { ExpandedTwoDimensionalArray(innerType, _) }
      case array @ WdlArray(WdlArrayType(nonArrayType), _) => Failure(new IllegalArgumentException(s"Array must be two-dimensional to be transposed but given array of $nonArrayType"))
      case otherValue => Failure(new IllegalArgumentException(s"Function 'transpose' must be given a two-dimensional array but instead got ${otherValue.typeName}"))
    }

    def expandWdlArray(outerArray: Seq[WdlValue]): Try[Seq[Seq[WdlValue]]] = Try {
      outerArray map {
        case array: WdlArray => array.value
        case otherValue => throw new IllegalArgumentException(s"Function 'transpose' must be given a two-dimensional array but instead got WdlArray[${otherValue.typeName}]")
      }
    }

    def transpose(expandedTwoDimensionalArray: ExpandedTwoDimensionalArray): Try[WdlArray] = Try {
      val innerType = expandedTwoDimensionalArray.innerType
      val array = expandedTwoDimensionalArray.value
      WdlArray(WdlArrayType(WdlArrayType(innerType)), array.transpose map { WdlArray(WdlArrayType(innerType), _) })
    }

    extractExactlyOneArg.flatMap(validateAndExpand).flatMap(transpose)
  }

  def range(params: Seq[Try[WdlValue]]): Try[WdlArray] = {
    def extractAndValidateArguments = params.size match {
      case 1 => validateArguments(params.head)
      case n => Failure(new IllegalArgumentException(s"Invalid number of parameters for engine function range: $n. Ensure range(x: WdlInteger) takes exactly 1 parameters."))
    }

    def validateArguments(value: Try[WdlValue]) = value match {
      case Success(intValue: WdlValue) if WdlIntegerType.isCoerceableFrom(intValue.wdlType) =>
        Integer.valueOf(intValue.valueString) match {
          case i if i >= 0 => Success(i)
          case n => Failure(new IllegalArgumentException(s"Parameter to seq must be greater than or equal to 0 (but got $n)"))
        }
      case _ => Failure(new IllegalArgumentException(s"Invalid parameter for engine function seq: $value."))
    }

    extractAndValidateArguments map { intValue => WdlArray(WdlArrayType(WdlIntegerType), (0 until intValue).map(WdlInteger(_))) }
  }

  def sub(params: Seq[Try[WdlValue]]): Try[WdlString] = {
    def extractArguments = params.size match {
      case 3 => Success((params.head, params(1), params(2)))
      case n => Failure(new IllegalArgumentException(s"Invalid number of parameters for engine function sub: $n. sub takes exactly 3 parameters."))
    }

    def validateArguments(values: (Try[WdlValue], Try[WdlValue], Try[WdlValue])) = values match {
      case (Success(strValue), Success(WdlString(pattern)), Success(replaceValue))
        if WdlStringType.isCoerceableFrom(strValue.wdlType) &&
          WdlStringType.isCoerceableFrom(replaceValue.wdlType) =>
        Success((strValue.valueString, pattern, replaceValue.valueString))
      case _ => Failure(new IllegalArgumentException(s"Invalid parameters for engine function sub: $values."))
    }

    for {
      args <- extractArguments
      (str, pattern, replace) <- validateArguments(args)
    } yield WdlString(pattern.r.replaceAllIn(str, replace))
  }

  /**
    * Asserts that the parameter list contains a single parameter which will be interpreted
    * as a File and attempts to read the contents of that file and returns back the contents
    * as a String
    */
  private def readContentsFromSingleFileParameter(params: Seq[Try[WdlValue]]): Try[String] = {
    for {
      singleArgument <- extractSingleArgument(params)
      string = fileContentsToString(singleArgument.valueString)
    } yield string
  }

  private def extractObjects(params: Seq[Try[WdlValue]]): Try[Array[WdlObject]] = for {
    contents <- readContentsFromSingleFileParameter(params)
    wdlObjects <- WdlObject.fromTsv(contents)
  } yield wdlObjects

  private def writeContent(baseName: String, content: String): Try[WdlFile] = {
    Try(WdlFile(writeTempFile(tempFilePath, s"$baseName.", ".tmp", content)))
  }

  private def writeToTsv(params: Seq[Try[WdlValue]], wdlClass: Class[_ <: WdlValue with TsvSerializable]) = {
    for {
      singleArgument <- extractSingleArgument(params)
      downcast <- Try(wdlClass.cast(singleArgument))
      tsvSerialized <- downcast.tsvSerialize
      file <- writeContent(wdlClass.getSimpleName.toLowerCase, tsvSerialized)
    } yield file
  }
}

class WdlStandardLibraryFunctionsType extends WdlFunctions[WdlType] {
  def stdout(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def stderr(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def read_lines(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlArrayType(WdlStringType))
  def read_tsv(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlArrayType(WdlArrayType(WdlStringType)))
  def read_map(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlMapType(WdlStringType, WdlStringType))
  def read_object(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlObjectType)
  def read_objects(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlArrayType(WdlObjectType))
  def read_json(params: Seq[Try[WdlType]]): Try[WdlType] = Failure(new WdlExpressionException("Return type of read_json() can't be known statically"))
  def read_int(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlIntegerType)
  def read_string(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlStringType)
  def read_float(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFloatType)
  def read_boolean(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlBooleanType)
  def write_lines(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def write_tsv(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def write_map(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def write_object(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def write_objects(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def write_json(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFileType)
  def glob(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlArrayType(WdlFileType))
  def size(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlFloatType)
  def sub(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlStringType)
  def range(params: Seq[Try[WdlType]]): Try[WdlType] = Success(WdlArrayType(WdlIntegerType))
  def transpose(params: Seq[Try[WdlType]]): Try[WdlType] = params.toList match {
    case Success(t @ WdlArrayType(WdlArrayType(wdlType))) :: Nil => Success(t)
    case _ => Failure(new Exception(s"Unexpected transpose target: $params"))
  }
}

case object NoFunctions extends WdlStandardLibraryFunctions {
  override def glob(path: String, pattern: String): Seq[String] = throw new NotImplementedError()
  override def readFile(path: String): String = throw new NotImplementedError()
  override def writeTempFile(path: String, prefix: String, suffix: String, content: String): String = throw new NotImplementedError()
  override def stdout(params: Seq[Try[WdlValue]]): Try[WdlFile] = Failure(new NotImplementedError())
  override def stderr(params: Seq[Try[WdlValue]]): Try[WdlFile] = Failure(new NotImplementedError())
  override def read_json(params: Seq[Try[WdlValue]]): Try[WdlValue] = Failure(new NotImplementedError())
  override def write_tsv(params: Seq[Try[WdlValue]]): Try[WdlFile] = Failure(new NotImplementedError())
  override def write_json(params: Seq[Try[WdlValue]]): Try[WdlFile] = Failure(new NotImplementedError())
  override def size(params: Seq[Try[WdlValue]]): Try[WdlFloat] = Failure(new NotImplementedError())
  override def sub(params: Seq[Try[WdlValue]]): Try[WdlString] = Failure(new NotImplementedError())
  override def range(params: Seq[Try[WdlValue]]): Try[WdlArray] = Failure(new NotImplementedError())
  override def transpose(params: Seq[Try[WdlValue]]): Try[WdlArray] = Failure(new NotImplementedError())
}
