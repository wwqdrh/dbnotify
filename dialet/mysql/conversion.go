package mysql

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/wwqdrh/logger"
)

type RowsEventData struct {
	BinlogEventHeader replication.EventHeader
	BinlogEvent       replication.RowsEvent
	TableMetadata     TableMetadata
}

func NewRowsEventData(binlogEventHeader replication.EventHeader, binlogEvent replication.RowsEvent, tableMetadata TableMetadata) RowsEventData {
	return RowsEventData{
		BinlogEventHeader: binlogEventHeader,
		BinlogEvent:       binlogEvent,
		TableMetadata:     tableMetadata,
	}
}

func ConvertQueryEventToMessage(binlogEventHeader replication.EventHeader, binlogEvent replication.QueryEvent) Message {
	header := NewMessageHeader(
		string(binlogEvent.Schema),
		"(unknown)",
		time.Unix(int64(binlogEventHeader.Timestamp), 0),
		binlogEventHeader.LogPos,
		0,
	)

	message := NewQueryMessage(
		header,
		SqlQuery(binlogEvent.Query),
	)

	return Message(message)
}

func ConvertRowsEventsToMessages(xId uint64, rowsEventsData []RowsEventData) []Message {
	var ret []Message

	for _, d := range rowsEventsData {
		rowData := mapRowDataDataToColumnNames(d.BinlogEvent.Rows, d.TableMetadata.Fields)

		header := NewMessageHeader(
			d.TableMetadata.Schema,
			d.TableMetadata.Table,
			time.Unix(int64(d.BinlogEventHeader.Timestamp), 0),
			d.BinlogEventHeader.LogPos,
			xId,
		)

		switch d.BinlogEventHeader.EventType {
		case replication.WRITE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2:
			for _, message := range createInsertMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}

			break

		case replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			for _, message := range createUpdateMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}

			break

		case replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:
			for _, message := range createDeleteMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}

			break

		default:
			logger.DefaultLogger.Error(fmt.Sprintf("Can't convert unknown event %s", d.BinlogEventHeader.EventType))

			break
		}
	}

	return ret
}

func createUpdateMessagesFromRowData(header MessageHeader, rowData []MessageRowData) []UpdateMessage {
	if len(rowData)%2 != 0 {
		panic("update rows should be old/new pairs") // should never happen as per mysql format
	}

	var ret []UpdateMessage
	var tmp MessageRowData

	for index, data := range rowData {
		if index%2 == 0 {
			tmp = data
		} else {
			ret = append(ret, NewUpdateMessage(header, tmp, data))
		}
	}

	return ret
}

func createInsertMessagesFromRowData(header MessageHeader, rowData []MessageRowData) []InsertMessage {
	var ret []InsertMessage

	for _, data := range rowData {
		ret = append(ret, NewInsertMessage(header, data))
	}

	return ret
}

func createDeleteMessagesFromRowData(header MessageHeader, rowData []MessageRowData) []DeleteMessage {
	var ret []DeleteMessage

	for _, data := range rowData {
		ret = append(ret, NewDeleteMessage(header, data))
	}

	return ret
}

func mapRowDataDataToColumnNames(rows [][]interface{}, columnNames map[int]string) []MessageRowData {
	var mappedRows []MessageRowData

	for _, row := range rows {
		data := make(map[string]interface{})
		unknownCount := 0

		detectedMismatch, mismatchNotice := detectMismatch(row, columnNames)

		for columnIndex, columnValue := range row {
			if detectedMismatch {
				data[fmt.Sprintf("(unknown_%d)", unknownCount)] = columnValue
				unknownCount++
			} else {
				columnName, exists := columnNames[columnIndex]

				if !exists {
					// This should actually never happen
					// Fail hard before doing anything weird
					panic(fmt.Sprintf("No mismatch between row and column names array detected, but column %s not found", columnName))
				}

				data[columnName] = columnValue
			}
		}

		if detectedMismatch {
			mappedRows = append(mappedRows, MessageRowData{Row: data, MappingNotice: mismatchNotice})
		} else {
			mappedRows = append(mappedRows, MessageRowData{Row: data})
		}
	}

	return mappedRows
}

func detectMismatch(row []interface{}, columnNames map[int]string) (bool, string) {
	if len(row) > len(columnNames) {
		return true, fmt.Sprintf("column names array is missing field(s), will map them as unknown_*")
	}

	if len(row) < len(columnNames) {
		return true, fmt.Sprintf("row is missing field(s), ignoring missing")
	}

	return false, ""
}
