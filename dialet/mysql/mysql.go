//go:build todo

package mysql

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/wwqdrh/logger"
)

type ConsumerFunc func(Message) error

func ParseBinlogToMessages(binlogFilename string, tableMap TableMap, consumer ConsumerFunc) error {
	rowRowsEventBuffer := NewRowsEventBuffer()

	p := replication.NewBinlogParser()

	f := func(e *replication.BinlogEvent) error {
		switch e.Header.EventType {
		case replication.QUERY_EVENT:
			queryEvent := e.Event.(*replication.QueryEvent)
			query := string(queryEvent.Query)

			if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
				logger.DefaultLogger.Info("Starting transaction")
			} else if strings.HasPrefix(strings.ToUpper(strings.Trim(query, " ")), "SAVEPOINT") {
				logger.DefaultLogger.Info("Skipping transaction savepoint")
			} else {
				logger.DefaultLogger.Info("Query event")

				err := consumer(ConvertQueryEventToMessage(*e.Header, *queryEvent))

				if err != nil {
					return err
				}
			}

			break

		case replication.XID_EVENT:
			xidEvent := e.Event.(*replication.XIDEvent)
			xId := uint64(xidEvent.XID)

			logger.DefaultLogger.Info(fmt.Sprintf("Ending transaction xID %d", xId))

			for _, message := range ConvertRowsEventsToMessages(xId, rowRowsEventBuffer.Drain()) {
				err := consumer(message)

				if err != nil {
					return err
				}
			}

			break

		case replication.TABLE_MAP_EVENT:
			tableMapEvent := e.Event.(*replication.TableMapEvent)

			schema := string(tableMapEvent.Schema)
			table := string(tableMapEvent.Table)
			tableId := uint64(tableMapEvent.TableID)

			err := tableMap.Add(tableId, schema, table)

			if err != nil {
				logger.DefaultLogger.Error(fmt.Errorf("Failed to add table information for table %s.%s (id %d)", schema, table, tableId).Error())
				return err
			}

			break

		case replication.WRITE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv2:
			rowsEvent := e.Event.(*replication.RowsEvent)

			tableId := uint64(rowsEvent.TableID)
			tableMetadata, ok := tableMap.LookupTableMetadata(tableId)

			if !ok {
				logger.DefaultLogger.Error(fmt.Sprintf("Skipping event - no table found for table id %d", tableId))
				break
			}

			rowRowsEventBuffer.BufferRowsEventData(
				NewRowsEventData(*e.Header, *rowsEvent, tableMetadata),
			)

			break

		default:
			break
		}

		return nil
	}

	return p.ParseFile(binlogFilename, 0, f)
}

type RowsEventBuffer struct {
	buffered []RowsEventData
}

func NewRowsEventBuffer() RowsEventBuffer {
	return RowsEventBuffer{}
}

func (mb *RowsEventBuffer) BufferRowsEventData(d RowsEventData) {
	mb.buffered = append(mb.buffered, d)
}

func (mb *RowsEventBuffer) Drain() []RowsEventData {
	ret := mb.buffered
	mb.buffered = nil

	return ret
}

type binlogParseFunc func(string) error

func createBinlogParseFunc(dbDsn string, consumerChain ConsumerChain) binlogParseFunc {
	return func(binlogFilename string) error {
		return parseBinlogFile(binlogFilename, dbDsn, consumerChain)
	}
}

func parseBinlogFile(binlogFilename, dbDsn string, consumerChain ConsumerChain) error {
	logger.DefaultLogger.Infox("Parsing binlog file %s", []interface{}{binlogFilename})

	db, err := GetDatabaseInstance(dbDsn)

	if err != nil {
		return err
	}

	defer db.Close()

	tableMap := NewTableMap(db)

	logger.DefaultLogger.Info("About to parse file ...")

	return ParseBinlog(binlogFilename, tableMap, consumerChain)
}
