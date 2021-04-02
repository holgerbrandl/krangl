package krangl

import org.apache.poi.ss.usermodel.*
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.XSSFCellStyle
import org.apache.poi.xssf.usermodel.XSSFSheet
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException

//private fun testExcelInCP() {
//    try {
//        Class.forName("org/apache/poi/ss/util/CellRangeAddress")
//    } catch (e: ClassNotFoundException) {
//        throw(RuntimeException("poi dependencies are missing in path. To enable excel support, please add 'org.apache.poi:poi-ooxml:4.1.1' to your project dependencies"))
//    }
//}

/**
 * Returns a DataFrame with the contents from an Excel file sheet
 */
@JvmOverloads
fun DataFrame.Companion.readExcel(
    path: String,
    sheet: Int = 0,
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
        val xlSheet = xlWBook.getSheetAt(sheet) ?: throw IOException("Sheet at index $sheet not found")
        df = readExcelSheet(xlSheet, cellRange, colTypes, trim_ws, guessMax, na, stopAtBlankLine, includeBlankLines)
    } finally {
        xlWBook.close()
        inputStream.close()
    }
    return df
}

/**
 * Returns a DataFrame with the contents from an Excel file sheet
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
    trim_ws: Boolean,
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
    var valueList: MutableList<String>

    // Get column names
    val columnResults = getExcelColumnNames(cellIterator, df, cellRange)
    df = columnResults.first
    cellRange.lastColumn = columnResults.second  // Stops at first empty column header

    //Get rows
    while (rowIterator.hasNext() && currentRow.rowNum < cellRange.lastRow) {
        currentRow = rowIterator.next()
        valueList = mutableListOf()
        val hasValues = readExcelRow(currentRow, valueList, cellRange, trim_ws, na)

        //Prevent Excel reading blank lines (whose contents have been cleared but the lines weren't deleted)
        if (hasValues)
            df = df.addRow(valueList)
        else
            if (stopAtBlankLine)
                break //Stops reading on first blank line
            else
                if (includeBlankLines)
                    df = df.addRow(valueList)
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
    valueList: MutableList<String>,
    cellRange: CellRangeAddress,
    trim_ws: Boolean,
    na: String,
): Boolean {
    val dataFormatter = DataFormatter()
    var cellCounter = 0
    var currentCell: Cell?
    var currentValue: String
    var hasValues = false
    while (cellCounter <= cellRange.lastColumn) { //iterator skips blank cells, this ensures all are read
        if (cellCounter < cellRange.firstColumn) {
            cellCounter++
            continue
        }
        currentCell = currentRow.getCell(cellCounter)

        currentValue = na
        currentCell?.let { currentValue = dataFormatter.formatCellValue(currentCell) }
        if (currentValue.isEmpty())
            currentValue = na
        if (trim_ws)
            currentValue = currentValue.trim()
        valueList.add(currentValue)
        cellCounter++

        if (currentValue != na)
            hasValues = true
    }
    return hasValues
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

    df.cols.forEachIndexed{ index, column ->
        colList.add(dataColFactory(column.name, (colTypes.typeOf(index, column.name)), column.values(), guessMax))
    }

    return SimpleDataFrame(colList)
}

fun DataFrame.writeExcel(
    filePath: String,
    sheetName: String,
    headers: Boolean = true,
    eraseFile: Boolean = false,
    boldHeaders: Boolean = true
) {
    val workbook: XSSFWorkbook = if (eraseFile)
        XSSFWorkbook()
    else {
        try {
            XSSFWorkbook(FileInputStream(filePath))
        } catch (e: FileNotFoundException) {
            XSSFWorkbook()
        }
    }
    val sheet: XSSFSheet
    //Let Exception be thrown if already exists
    sheet = workbook.createSheet(sheetName)

    val headerCellStyle =
        if (headers)
            createExcelHeaderStyle(workbook, boldHeaders)
        else
            workbook.createCellStyle()
    if (headers)
        createExcelHeaderRow(sheet, boldHeaders, headerCellStyle)
    createExcelDataRows(sheet, headers)
    // Exception will be thrown if file is already open
    val fileOut = FileOutputStream(filePath)
    workbook.write(fileOut)
    fileOut.close()
    workbook.close()
}

private fun createExcelHeaderStyle(
    workbook: XSSFWorkbook,
    boldHeaders: Boolean,
): XSSFCellStyle? {
    val headerCellStyle = workbook.createCellStyle()
    if (boldHeaders) {
        val headerFont = workbook.createFont()
        headerFont.bold = true
        headerFont.color = IndexedColors.BLACK.getIndex()
        headerCellStyle.setFont(headerFont)
    }
    return headerCellStyle
}

private fun DataFrame.createExcelDataRows(sheet: XSSFSheet, headers: Boolean) {
    var rowIdx = if (headers) 1 else 0
    for (row in this.rows) {
        val nRow = sheet.createRow(rowIdx++)
        for ((columnPosition, cell) in row.values.toMutableList().withIndex()) {
            nRow.createCell(columnPosition).setCellValue(cell.toString())
        }
    }
}

private fun DataFrame.createExcelHeaderRow(
    sheet: XSSFSheet,
    boldHeaders: Boolean,
    headerCellStyle: XSSFCellStyle?
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