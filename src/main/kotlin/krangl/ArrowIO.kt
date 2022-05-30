package krangl

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BaseFixedWidthVector
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.SmallIntVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.arrow.vector.util.Text
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.channels.*
import java.nio.file.StandardOpenOption
import java.util.*

internal fun unwrapStringArrayFromArrow(vector: VarCharVector): ArrayList<String?> {
    val result = ArrayList<String?>()
    for (i in 0 until vector.valueCount) {
        result.add(vector.getObject(i)?.toString())
    }
    return result
}

internal inline fun <reified ELEMENT_TYPE>unwrapNumericVectorFromArrow(vector: BaseFixedWidthVector, elementClass: Class<ELEMENT_TYPE>): List<ELEMENT_TYPE?> {
    val elements = vector.valueCount
    val outVector = ArrayList<ELEMENT_TYPE?>(elements)
    for (i in 0 until elements) {
        outVector.add(vector.getObject(i) as ELEMENT_TYPE?)
    }
    return outVector
}

internal fun unwrapBooleanArrayFromArrow(vector: BitVector): ArrayList<Boolean?> {
    val result = ArrayList<Boolean?>()
    for (i in 0 until vector.valueCount) {
        result.add(vector.getObject(i))
    }
    return result
}

fun DataFrame.Companion.arrowReader() = ArrowReader()

class ArrowReader() {
    /**
     * Internal low-level function.
     * Use this function if you are working with [VectorSchemaRoot]s directly in your project.
     */
    fun fromVectorSchemaRoot(vectorSchemaRoot: VectorSchemaRoot): DataFrame {
        val kranglVectors = vectorSchemaRoot.fieldVectors.map { fieldVector ->
            when (fieldVector.field.type) {
                is ArrowType.FixedSizeList, is ArrowType.List -> {
                    throw Exception("Matrices are not supported yet")
                }
                is ArrowType.Utf8 -> {
                    StringCol(fieldVector.name, unwrapStringArrayFromArrow(fieldVector as VarCharVector))
                }
                is ArrowType.Int -> {
                    val bitWidth = (fieldVector.field.type as ArrowType.Int).bitWidth
                    when (bitWidth) {
                        8 ->  IntCol(fieldVector.name, unwrapNumericVectorFromArrow(fieldVector as TinyIntVector, Int::class.java))
                        16 -> IntCol(fieldVector.name, unwrapNumericVectorFromArrow(fieldVector as SmallIntVector, Int::class.java))
                        32 -> IntCol(fieldVector.name, unwrapNumericVectorFromArrow(fieldVector as IntVector, Int::class.java))
                        64 -> LongCol(fieldVector.name, unwrapNumericVectorFromArrow(fieldVector as BigIntVector, Long::class.java))
                        else -> throw java.lang.Exception("Incorrect Int.bitWidth ($bitWidth, should never happen)")
                    }
                }
                is ArrowType.FloatingPoint -> {
                    val precision = (fieldVector.field.type as ArrowType.FloatingPoint).precision
                    when (precision) {
                        FloatingPointPrecision.HALF -> java.lang.Exception("HALF float not supported")
                        FloatingPointPrecision.SINGLE -> DoubleCol(fieldVector.name, unwrapNumericVectorFromArrow(fieldVector as Float4Vector, Double::class.java))
                        FloatingPointPrecision.DOUBLE -> DoubleCol(fieldVector.name, unwrapNumericVectorFromArrow(fieldVector as Float8Vector, Double::class.java))
                        else -> throw java.lang.Exception("Incorrect FloatingPoint.precision ($precision, should never happen)")
                    }
                }
                is ArrowType.Bool -> {
                    BooleanCol(fieldVector.name, unwrapBooleanArrayFromArrow(fieldVector as BitVector))
                }
                else -> {
                    throw Exception("${fieldVector.field.type.typeID.name} is not supported yet")
                }
            }
        }

        return dataFrameOf(*(kranglVectors as List<DataCol>).toTypedArray())
    }

    /**
     * Read [VectorSchemaRoot] from existing [channel] and convert it to [DataFrame].
     * Use this function if you want to manage channels yourself, make in-memory IPC sharing and so on.
     * If [allocator] is null, it will be created and closed inside.
     */
    fun readFromChannel(channel: SeekableByteChannel, allocator: BufferAllocator?): DataFrame {
        fun readFromChannelAllocating(channel: SeekableByteChannel, allocator: BufferAllocator?): DataFrame {
            ArrowFileReader(channel, allocator).use { reader ->
                reader.loadNextBatch()
                return fromVectorSchemaRoot(reader.vectorSchemaRoot)
            }
        }
        if (allocator == null ) {
            RootAllocator().use { newAllocator ->
                return readFromChannelAllocating(channel, newAllocator)
            }
        } else {
            return readFromChannelAllocating(channel, allocator)
        }
    }

    /**
     * Read [VectorSchemaRoot] from ByteArray and convert it to [DataFrame].
     */
    fun fromByteArray(byteArray: ByteArray): DataFrame {
        return readFromChannel(ByteArrayReadableSeekableByteChannel(byteArray), null)
    }

    /**
     * Read [VectorSchemaRoot] from [file] by and convert it to [DataFrame].
     */
    fun fromFile(file: File): DataFrame {
        if (!file.exists()) {
            throw Exception("${file.path} does not exist")
        }
        if (file.isDirectory) {
            throw Exception("${file.path} is directory")
        }
        FileChannel.open(
            file.toPath(),
            StandardOpenOption.READ
        ).use { channel ->
            return readFromChannel(channel, null)
        }
    }

    /**
     * Read [VectorSchemaRoot] from file by [path] and convert it to [DataFrame].
     */
    fun fromFile(path: String): DataFrame {
        return fromFile(File(path))
    }
}

fun DataFrame.arrowWriter() = ArrowWriter(this)

class ArrowWriter(val dataFrame: DataFrame) {
    internal fun fromStringCol(column: StringCol, allocator: BufferAllocator): VarCharVector {
        val fieldVector = VarCharVector(column.name, allocator)
        fieldVector.allocateNew(column.length)
        column.values.forEachIndexed { index, value ->
            if (value == null) {
                fieldVector.setNull(index)
            } else {
                fieldVector.setSafe(index, Text(value))
            }
        }
        fieldVector.valueCount = column.length
        return fieldVector
    }

    internal fun fromBooleanCol(column: BooleanCol, allocator: BufferAllocator): BitVector {
        val fieldVector = BitVector(column.name, allocator)
        fieldVector.allocateNew(column.length)
        column.values.forEachIndexed { index, value ->
            if (value == null) {
                fieldVector.setNull(index)
            } else {
                fieldVector.setSafe(index, if (value) 1 else 0)
            }
        }
        fieldVector.valueCount = column.length
        return fieldVector
    }

    internal fun fromIntCol(column: IntCol, allocator: BufferAllocator): IntVector {
        val fieldVector = IntVector(column.name, allocator)
        fieldVector.allocateNew(column.length)
        column.values.forEachIndexed { index, value ->
            if (value == null) {
                fieldVector.setNull(index)
            } else {
                fieldVector.setSafe(index, value)
            }
        }
        fieldVector.valueCount = column.length
        return fieldVector
    }

    internal fun fromLongCol(column: LongCol, allocator: BufferAllocator): BigIntVector {
        val fieldVector = BigIntVector(column.name, allocator)
        fieldVector.allocateNew(column.length)
        column.values.forEachIndexed { index, value ->
            if (value == null) {
                fieldVector.setNull(index)
            } else {
                fieldVector.setSafe(index, value)
            }
        }
        fieldVector.valueCount = column.length
        return fieldVector
    }

    internal fun fromDoubleCol(column: DoubleCol, allocator: BufferAllocator): Float8Vector {
        val fieldVector = Float8Vector(column.name, allocator)
        fieldVector.allocateNew(column.length)
        column.values.forEachIndexed { index, value ->
            if (value == null) {
                fieldVector.setNull(index)
            } else {
                fieldVector.setSafe(index, value)
            }
        }
        fieldVector.valueCount = column.length
        return fieldVector
    }

    internal fun fromAnyCol(column: AnyCol, allocator: BufferAllocator): VarCharVector {
        val fieldVector = VarCharVector(column.name, allocator)
        fieldVector.allocateNew(column.length)
        column.values.forEachIndexed { index, value ->
            if (value == null) {
                fieldVector.setNull(index)
            } else {
                fieldVector.setSafe(index, Text(value.toString()))
            }
        }
        fieldVector.valueCount = column.length
        return fieldVector
    }

    /**
     * Internal low-level function.
     * Use this function if you are working with [VectorSchemaRoot]s and [BufferAllocator]s directly in your project.
     */
    fun allocateVectorSchemaRoot(allocator: BufferAllocator): VectorSchemaRoot {
        val arrowVectors = dataFrame.cols.map { column ->
            when (column) {
                is StringCol -> fromStringCol(column, allocator)
                is BooleanCol -> fromBooleanCol(column, allocator)
                is IntCol -> fromIntCol(column, allocator)
                is LongCol -> fromLongCol(column, allocator)
                is DoubleCol -> fromDoubleCol(column, allocator)
                is AnyCol -> fromAnyCol(column, allocator)
                else -> {
                    throw Exception("Unknown column type ${column.javaClass.canonicalName}")
                }
            }
        }
        return VectorSchemaRoot(arrowVectors)
    }

    /**
     * Export [dataFrame] to [VectorSchemaRoot] and write it to any existing [channel].
     * Use this function if you want to manage channels yourself, make in-memory IPC sharing and so on
     */
    fun writeToChannel(channel: WritableByteChannel) {
        RootAllocator().use { allocator ->
            this.allocateVectorSchemaRoot(allocator).use { vectorSchemaRoot ->
                ArrowFileWriter(vectorSchemaRoot, null, channel).use { writer ->
                    writer.writeBatch();
                }
            }
        }
    }

    /**
     * Export [dataFrame] to [VectorSchemaRoot] and write it to new ByteArray.
     */
    fun toByteArray(): ByteArray {
        ByteArrayOutputStream().use { byteArrayStream ->
            Channels.newChannel(byteArrayStream).use { channel ->
                writeToChannel(channel)
                return byteArrayStream.toByteArray()
            }
        }
    }

    /**
     * Export [dataFrame] to [VectorSchemaRoot] and write it to new or existing [file].
     * Temporary file is created if [file] argument is null.
     */
    fun toFile(file: File?): File {
        val saveToFile = file ?: File.createTempFile("DataFrame", ".arrow")

        FileChannel.open(
            saveToFile.toPath(),
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE
        ).use { channel ->
            channel.truncate(0)
            writeToChannel(channel)
        }
        return saveToFile
    }

    /**
     * Export [dataFrame] to [VectorSchemaRoot] and write it to new or existing file by [path].
     * Temporary file is created if [path] argument is null.
     */
    fun toFile(path: String?): File {
        val saveToFile = if (path != null) {
            File(path)
        } else {
            File.createTempFile("DataFrame", ".arrow")
        }
        return toFile(saveToFile)
    }
}
