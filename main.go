package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

type Args struct {
	TableName      string
	InputFileName  string
	SchemaFileName string
}

type Schema struct {
	ColumnFrom   string
	DataTypeFrom string
	ColumnTo     string
	DataTypeTo   string
}

func main() {
	args, err := ParseArgs(os.Args)
	if err != nil {
		fmt.Println(err)
		return
	}

	schema, err := ReadSchema(args.SchemaFileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	reader, err := ReadInputFile(args.InputFileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	headers, err := ParseHeaders(reader)
	if err != nil {
		fmt.Println(err)
		return
	}

	headerIndexMap := MapHeadersToSchema(headers, schema)

	outputSQL := GenerateSQL(args.TableName, schema, headerIndexMap, reader)

	if err := WriteSQLToFile(outputSQL, args.TableName); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("SQL file %s.SQL has been generated successfully.\n", args.TableName)
}

func ParseArgs(args []string) (*Args, error) {
	if len(args) < 4 {
		return nil, fmt.Errorf("usage: convert [table name] [input file name] [schema info CSV file name]")
	}

	return &Args{
		TableName:      args[1],
		InputFileName:  args[2],
		SchemaFileName: args[3],
	}, nil
}

func ReadSchema(schemaFileName string) ([]Schema, error) {
	schemaFile, err := os.Open(schemaFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open schema file: %s", err)
	}
	defer schemaFile.Close()

	schemaReader := csv.NewReader(schemaFile)
	schema, err := schemaReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %s", err)
	}

	var result []Schema
	for _, column := range schema {
		result = append(result, Schema{
			ColumnFrom:   column[0],
			DataTypeFrom: column[1],
			ColumnTo:     column[2],
			DataTypeTo:   column[3],
		})
	}

	return result, nil
}

func ReadInputFile(inputFileName string) (io.Reader, error) {
	inputFile, err := os.Open(inputFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %s", err)
	}
	defer inputFile.Close()

	b := make([]byte, 3)
	if _, err := inputFile.Read(b); err != nil {
		return nil, fmt.Errorf("failed to read first 3 bytes of input file: %s", err)
	}
	b = removeBOM(b)

	return io.MultiReader(bytes.NewReader(b), inputFile), nil
}

func ParseHeaders(reader io.Reader) ([]string, error) {
	csvReader := csv.NewReader(reader)
	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read headers from input file: %s", err)
	}

	return headers, nil
}

func MapHeadersToSchema(headers []string, schema []Schema) map[string]int {
	headerIndexMap := make(map[string]int)
	for i, header := range headers {
		headerIndexMap[header] = i
	}
	return headerIndexMap
}

func GenerateSQL(tableName string, schema []Schema, headerIndexMap map[string]int, reader io.Reader) string {
	inputReader := csv.NewReader(reader)

	var outputSQL strings.Builder
	outputSQL.WriteString(fmt.Sprintf("INSERT INTO `%s` (", tableName))

	columns := make([]string, 0, len(schema))
	for _, column := range schema {
		columns = append(columns, fmt.Sprintf("`%s`", column.ColumnTo))
	}
	outputSQL.WriteString(strings.Join(columns, ", "))
	outputSQL.WriteString(")\nVALUES\n")

	for i := 0; ; i++ {
		row, err := inputReader.Read()
		if err == io.EOF {
			outputSQL.WriteString(";\n")
			break
		}
		if err != nil {
			fmt.Printf("failed to read row %d: %s\n", i, err)
			continue
		}

		if i > 0 {
			outputSQL.WriteString(",\n")
		}

		outputSQL.WriteString("(")
		for j, column := range schema {
			headerIndex := headerIndexMap[column.ColumnFrom]
			value := row[headerIndex]

			convertedValue := convertData(value, column.DataTypeFrom, column.DataTypeTo)

			outputSQL.WriteString(fmt.Sprintf("'%s'", convertedValue))
			if j < len(schema)-1 {
				outputSQL.WriteString(", ")
			}
		}
		outputSQL.WriteString(")")
	}

	return outputSQL.String()
}

func WriteSQLToFile(sql, tableName string) error {
	outputFileName := fmt.Sprintf("%s.SQL", tableName)
	return os.WriteFile(outputFileName, []byte(sql), 0644)
}

func removeBOM(data []byte) []byte {
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return data[3:]
	}
	return data
}

func convertData(value, srcType, destType string) string {
	switch srcType {
	case "int":
		switch destType {
		case "BIGINT":
			return value // MySQLのBIGINTとして扱う
		case "VARCHAR":
			return value // 文字列として扱う
		}
	case "nvarchar", "varchar":
		return value // 基本的にそのまま文字列として扱う
	case "datetime":
		return value // MySQLのDATETIMEに対応
	}
	return value
}
