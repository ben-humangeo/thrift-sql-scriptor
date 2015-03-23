package com.benjamindparrish.thrift.sql;

import com.google.common.base.CaseFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by bparrish on 3/23/15.
 */
public class ThriftParser {

    private static final Logger _logger = LoggerFactory.getLogger(ThriftParser.class);

    private static final String LINE_SEPARATOR = "\n";
    private static final String TAB = "\t";

    private static final String STRUCT = "struct";
    private static final String ENUM = "enum";
    private static final String REQUIRED = "required";

    private static final String ENUM_ID_COLUMN = "ID";
    private static final String ENUM_VALUE_COLUMN = "VALUE";

    private final File _file;
    private String _outputFilePath;
    private String _fileName;

    private boolean inTable;

    public ThriftParser(String filePath, String outputFilePath, String fileName) {
        _file = new File(filePath);
        _outputFilePath = (outputFilePath == null) ? FilenameUtils.getFullPath(filePath): outputFilePath;
        _fileName = (fileName == null) ? FilenameUtils.removeExtension(_file.getName()) + ".sql" : fileName;
    }

    public boolean parse() throws IOException {
        BufferedWriter sqlWriter = getWriter(_outputFilePath, _fileName);

        BufferedReader sqlReader = getReader();

        if (sqlReader == null) {
            return false;
        }

        String line = sqlReader.readLine();
        String nextLine;

        boolean nextIsEndTable = false;

        while ((nextLine = sqlReader.readLine()) != null) {
            if (nextLine.trim().equals("}")) {
                nextIsEndTable = true;
            } else {
                nextIsEndTable = false;
            }

            if (line.isEmpty()) {
                line = nextLine;

                continue;
            }

            line = line.trim();

            String[] tokens = line.split(" ");

            ParseSQL parseSQL = parseTokens(tokens);

            switch (parseSQL) {
                case TABLE:
                    createTable(tokens, sqlWriter);
                    break;
                case CLOSE_TABLE:
                    closeTable(sqlWriter);
                    break;
                case LOOKUP:
                    createLookup(tokens, sqlWriter, sqlReader);
                    break;
                case COLUMN:
                    createColumn(tokens, sqlWriter, nextIsEndTable);
                    break;
                case NONE:
                default:
                    break;
            }

            line = nextLine;
        }

        line = line.trim();

        String[] tokens = line.split(" ");

        ParseSQL parseSQL = parseTokens(tokens);

        switch (parseSQL) {
            case TABLE:
                createTable(tokens, sqlWriter);
                break;
            case CLOSE_TABLE:
                closeTable(sqlWriter);
                break;
            case LOOKUP:
                createLookup(tokens, sqlWriter, sqlReader);
                break;
            case COLUMN:
                createColumn(tokens, sqlWriter, nextIsEndTable);
                break;
            case NONE:
            default:
                break;
        }

        try {
            sqlWriter.close();
        } catch (IOException e) {
            _logger.error("failed to close the buffered writer", e);
        }

        return true;
    }

    private BufferedReader getReader() {
        FileReader sqlReader = null;
        try {
            sqlReader = new FileReader(_file);
        } catch (FileNotFoundException e) {
            _logger.error("failed to create a reader for Thrift definition file", e);

            return null;
        }

        return new BufferedReader(sqlReader);
    }

    private BufferedWriter getWriter(String outputFilePath, String fileName) throws IOException {
        FileOutputStream sqlOutputStream = null;
        try {
            File outputFile = new File(outputFilePath + fileName);

            if (outputFile.exists()) {
                FileUtils.forceDelete(outputFile);
            }

            sqlOutputStream = FileUtils.openOutputStream(outputFile);
        } catch (IOException e) {
            _logger.error("failed to create file - {}{}", outputFilePath, fileName, e);

            throw new IOException(e);
        }

        OutputStreamWriter sqlOutputStreamWriter = new OutputStreamWriter(sqlOutputStream);

        return new BufferedWriter(sqlOutputStreamWriter);
    }

    private ParseSQL parseTokens(String[] tokens) {
        if (tokens.length == 0) {
            return ParseSQL.NONE;
        }

        if (tokens[0].equals(STRUCT)) {
            return ParseSQL.TABLE;
        } else if (tokens[0].equals(ENUM)) {
            return ParseSQL.LOOKUP;
        } else if (Character.isDigit(tokens[0].charAt(0))) {
            return ParseSQL.COLUMN;
        } else if (tokens[0].charAt(0) == '}') {
            return ParseSQL.CLOSE_TABLE;
        } else {
            return ParseSQL.NONE;
        }
    }

    private void createTable(String[] tokens, BufferedWriter sqlWriter) throws IOException {
        _logger.debug("creating table - {}", tokens[1]);

        String str = String.format("CREATE TABLE `%s` (" + LINE_SEPARATOR, tokens[1]);

        sqlWriter.write(str);

        inTable = true;
    }
    
    private void createLookupTable(String[] tokens, BufferedWriter sqlWriter) throws IOException {
        _logger.debug("creating lookup table - {}", tokens[1]);

        createTable(tokens, sqlWriter);
        
        String str =
                TAB + ENUM_ID_COLUMN + " INT NOT NULL AUTO_INCREMENT PRIMARY KEY," + LINE_SEPARATOR +
                TAB + ENUM_VALUE_COLUMN + " VARCHAR(255) NOT NULL" + LINE_SEPARATOR;
        
        sqlWriter.write(str);
        
        closeTable(sqlWriter);
    }

    private void closeTable(BufferedWriter sqlWriter) throws IOException {
        sqlWriter.write(");" + LINE_SEPARATOR + LINE_SEPARATOR);

        inTable = false;
    }

    private void createLookup(String[] tokens, BufferedWriter sqlWriter, BufferedReader sqlReader) throws IOException {
        createLookupTable(tokens, sqlWriter);

        String line;

        while ((line = sqlReader.readLine()) != null) {
            line = line.trim();

            if (line.equals("}")) {
                break;
            }

            boolean isComma = line.charAt(line.length() - 1) == ',';

            if (isComma) {
                line = line.substring(0, line.length() - 1);
            }

            String str = String.format("INSERT INTO %s (`%s`) VALUES ('%s');", tokens[1], ENUM_VALUE_COLUMN, line);

            sqlWriter.write(str + LINE_SEPARATOR);
        }
    }

    private void createColumn(String[] tokens, BufferedWriter sqlWriter, boolean nextIsEndTable) throws IOException {
        boolean isRequired = tokens[1].equals(REQUIRED);
        String type = getColumnType(tokens[2]);
        String name = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, tokens[3].substring(0, tokens[3].length() - 1));

        String requiredValue = (isRequired) ? "NOT NULL" : "";
        String comma = (!nextIsEndTable) ? "," : "";

        String str = String.format(TAB + "`%s` %s %s%s", name, type, requiredValue, comma);

        sqlWriter.write(str + LINE_SEPARATOR);
    }

    private String getColumnType(String token) {
        if (token.equals("string")) {
            return "VARCHAR(255)";
        } else if (token.equals("i16")) {
            return "SMALLINT";
        } else if (token.equals("i32")) {
            return "INT";
        } else if (token.equals("i64")) {
            return "BIGINT";
        } else if (token.equals("double")) {
            return "DOUBLE";
        } else if (token.equals("bool")) {
            return "BIT";
        } else {
            _logger.debug("found new column type - {}", token);

            return "NULL_VALUE";
        }
    }

    private enum ParseSQL {
        NONE,
        TABLE,
        COLUMN,
        LOOKUP,
        CLOSE_TABLE
    }

}
