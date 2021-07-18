package krangl

import org.apache.poi.openxml4j.exceptions.InvalidOperationException
import org.apache.poi.ss.usermodel.*
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.*
import java.lang.Double.isInfinite
import kotlin.math.floor

//private fun testExcelInCP() {
//    try {
//        Class.forName("org/apache/poi/ss/util/CellRangeAddress")
//    } catch (e: ClassNotFoundException) {
//        throw(RuntimeException("poi dependencies are missing in path. To enable excel support, please add 'org.apache.poi:poi-ooxml:4.1.1' to your project dependencies"))
//    }
//}

/**
 * Returns a DataFrame with the contents from an Excel file sheet.  By default, krangl treats blank cells as missing data.
 */
@JvmOverloads
fun DataFrame.Companion.readExcel(
    path: String,
    sheet: Int = 0,
    cellRange: CellRangeAddress? = null,
    colTypes: ColumnTypeSpec = GuessSpec(),
    trim: Boolean = false,
    guessMax: Int = 100,
    na: String = MISSING_VALUE,
    stopAtBlankLine: Boolean = true,
    includeBlankLines: Boolean = false,
): DataFrame {
    val df: DataFrame
    val inputStream = FileInputStream(path)
    val xlWBook = WorkbookFactory.create(inputStream)
    try {
        val xlSheet = xlWBook.getSheetAt(sheet) ?: throw IOException("Sheet at index $sheet not found")
        df = readExcelSheet(xlSheet, cellRange, colTypes, trim, guessMax, na, stopAtBlankLine, includeBlankLines)
    } finally {
        xlWBook.close()
        inputStream.close()
    }
    return df
}

/**
 * Returns a DataFrame with the contents from an Excel file sheet. By default, krangl treats blank cells as missing data.
 */
@JvmOverloads
fun DataFrame.Companion.readExcel(
    path: String,
    sheet: String,
    cellRange: CellRangeAddress? = null,
    colTypes: ColumnTypeSpec = GuessSpec(),
    trim_ws: Boolean = false,
    guessMax: Int = 100,
    na: String = MISSING_VALUE,
    stopAtBlankLine: Boolean = true,
    includeBlankLines: Boolean = false,
): DataFrame {
    val df: DataFrame
    val inputStream = FileInputStream(path)
    val xlWBook = WorkbookFactory.create(inputStream)
    try {
        val xlSheet = xlWBook.getSheet(sheet) ?: throw IOException("Sheet $sheet not found")
        df = readExcelSheet(xlSheet, cellRange, colTypes, trim_ws, guessMax, na, stopAtBlankLine, includeBlankLines)
    } finally {
        xlWBook.close()
        inputStream.close()
    }
    return df
}

private fun readExcelSheet(
    xlSheet: Sheet,
    range: CellRangeAddress?,
    colTypes: ColumnTypeSpec = GuessSpec(),
    trim: Boolean,
    guessMax: Int,
    na: String,
    stopAtBlankLine: Boolean,
    includeBlankLines: Boolean
): DataFrame {
    var df = emptyDataFrame()
    val rowIterator = xlSheet.rowIterator()
    val cellRange = range ?: getDefaultCellAddress(xlSheet)

    if (!rowIterator.hasNext())
        return df

    // Skip lines until starting row number
    var currentRow = rowIterator.next()
    while (currentRow.rowNum < cellRange.firstRow - 1) {
        if (!rowIterator.hasNext())
            return df
        else {
            currentRow = rowIterator.next()
        }
    }

    val cellIterator = currentRow.iterator()

    // Get column names
    val columnResults = getExcelColumnNames(cellIterator, df, cellRange)
    df = columnResults.first
    cellRange.lastColumn = columnResults.second  // Stops at first empty column header

    //Get rows
    while (rowIterator.hasNext() && currentRow.rowNum < cellRange.lastRow) {
        currentRow = rowIterator.next()
        val values = readExcelRow(currentRow, cellRange, trim, na)

        //Prevent Excel reading blank lines (whose contents have been cleared but the lines weren't deleted)
        if (values.filterNotNull().isNotEmpty())
            df = df.addRow(values)
        else
            if (stopAtBlankLine)
                break //Stops reading on first blank line
            else
                if (includeBlankLines)
                    df = df.addRow(values)
    }
    return assignColumnTypes(df, colTypes, guessMax)
}

private fun getDefaultCellAddress(xlSheet: Sheet): CellRangeAddress {

    return CellRangeAddress(
        xlSheet.firstRowNum,
        xlSheet.lastRowNum,
        xlSheet.getRow(xlSheet.firstRowNum).firstCellNum.toInt(),
        xlSheet.getRow(xlSheet.firstRowNum).lastCellNum.toInt()
    )
}

private fun readExcelRow(
    currentRow: Row,
    cellRange: CellRangeAddress,
    trim: Boolean,
    na: String,
): List<Any?> {
    val dataFormatter = DataFormatter()
    var cellCounter = 0

    val rowValues = mutableListOf<Any?>()

    while (cellCounter <= cellRange.lastColumn) { //iterator skips blank cells, this ensures all are read
        if (cellCounter < cellRange.firstColumn) {
            cellCounter++
            continue
        }

        val currentCell = currentRow.getCell(cellCounter)

        var currentValue: Any? = currentCell?.cellType?.let {
            when (it) {
                CellType.NUMERIC -> {
                    val numValue = currentCell.numericCellValue
                    if(floor(numValue) == numValue && !isInfinite(numValue)) numValue.toLong() else numValue
                }
                CellType.STRING -> currentCell.stringCellValue
                CellType.BLANK -> null
                CellType.BOOLEAN -> currentCell.booleanCellValue
                CellType._NONE, CellType.ERROR, CellType.FORMULA -> dataFormatter.formatCellValue(currentCell)
            }
        }
//        var currentValue = currentCell?.let { dataFormatter.formatCellValue(currentCell) }

        if (currentValue is String) {
            if (trim) {
                currentValue = currentValue.trim()
            }

            if (currentValue == na) {
                currentValue = null
            }

            currentValue = (currentValue as String?)?.ifBlank { null }
        }

        rowValues.add(currentValue)
        cellCounter++
    }

    return rowValues
}

private fun getExcelColumnNames(
    cellIterator: MutableIterator<Cell>,
    df: DataFrame,
    cellRange: CellRangeAddress
): Pair<DataFrame, Int> {
    var currentCell: Cell?
    var lastColumn = cellRange.firstColumn
    var df1 = df
    while (cellIterator.hasNext()) {
        currentCell = cellIterator.next()

        if (currentCell.columnIndex < cellRange.firstColumn)
            continue
        else if (currentCell.columnIndex > cellRange.lastColumn)
            break

        if (currentCell.columnIndex > lastColumn + 1) break // Found empty column
        df1 = df1.addColumn(currentCell.toString()) { }
        lastColumn = currentCell.columnIndex
    }
    return Pair(df1, lastColumn)
}

private fun assignColumnTypes(df: DataFrame, colTypes: ColumnTypeSpec, guessMax: Int = 100): DataFrame {

    val colList = mutableListOf<DataCol>()

    df.cols.forEachIndexed { index, column ->
        colList.add(dataColFactory(column.name, (colTypes.typeOf(index, column.name)), column.values(), guessMax))
    }

    return SimpleDataFrame(colList)
}

fun DataFrame.writeExcel(
    filePath: String,
    sheetName: String,
    headers: Boolean = true,
    eraseFile: Boolean = false,
    boldHeaders: Boolean = true,
    rowAccessWindowSize: Int = 256
) {
    val workbook: Workbook = if (eraseFile)
        SXSSFWorkbook(rowAccessWindowSize)
    else {
        try {
            SXSSFWorkbook(XSSFWorkbook(FileInputStream(filePath)), rowAccessWindowSize)
        } catch (e: FileNotFoundException) {
            SXSSFWorkbook(rowAccessWindowSize)
        } catch (e: InvalidOperationException) {
            SXSSFWorkbook(rowAccessWindowSize)
        }
    }
    //Let Exception be thrown if already exists
    val sheet: Sheet = workbook.createSheet(sheetName)

    val headerCellStyle = if (headers) {
        createExcelHeaderStyle(workbook, boldHeaders)
    } else {
        workbook.createCellStyle()
    }

    if (headers) {
        createExcelHeaderRow(sheet, boldHeaders, headerCellStyle)
    }

    createExcelDataRows(sheet, headers)

    // Exception will be thrown if file is already open
    val fileOut = FileOutputStream(filePath)
    workbook.write(fileOut)
    fileOut.close()
    workbook.close()
}

private fun createExcelHeaderStyle(
    workbook: Workbook,
    boldHeaders: Boolean,
): CellStyle? {
    val headerCellStyle = workbook.createCellStyle()
    if (boldHeaders) {
        val headerFont = workbook.createFont()
        headerFont.bold = true
        headerFont.color = IndexedColors.BLACK.getIndex()
        headerCellStyle.setFont(headerFont)
    }
    return headerCellStyle
}

private fun DataFrame.createExcelDataRows(sheet: Sheet, headers: Boolean) {
    var rowIdx = if (headers) 1 else 0

    for (dfRow in rows) {
        val nRow = sheet.createRow(rowIdx++)

        for ((columnIndex, cellValue) in dfRow.values.toMutableList().withIndex()) {
            val cell = nRow.createCell(columnIndex)

            when (cols[columnIndex]) {
                is BooleanCol -> {
                    cell.cellType = CellType.BOOLEAN
                    cellValue?.let { cell.setCellValue(it as Boolean) }
                }
                is DoubleCol -> {
                    cell.cellType = CellType.NUMERIC
                    cellValue?.let { cell.setCellValue(it as Double) }
                }
                is IntCol -> {
                    cell.cellType = CellType.NUMERIC
                    cellValue?.let { cell.setCellValue((it as Int).toDouble()) }
                }
//                is StringCol -> cell.cellType= CellType.STRING
                else -> {
                    cellValue?.let { cell.setCellValue(cellValue.toString()) }
                }
            }
//            cell.setCellValue(cell.toString())
        }
    }
}

private fun DataFrame.createExcelHeaderRow(
    sheet: Sheet,
    boldHeaders: Boolean,
    headerCellStyle: CellStyle?
) {
    val headerRow = sheet.createRow(0)
    for ((colPos, col) in this.names.withIndex()) {
        val cell = headerRow.createCell(colPos)
        cell.setCellValue(col)
        if (boldHeaders)
            cell.cellStyle = headerCellStyle
    }
}

fun main() {
    DataFrame.readExcel("src/test/resources/krangl/data/ExcelReadExample.xlsx")
}