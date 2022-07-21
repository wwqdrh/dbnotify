package mysql

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/wwqdrh/logger"
)

func ParseBinlog(binlogFilename string, tableMap TableMap, consumerChain ConsumerChain) error {
	if _, err := os.Stat(binlogFilename); os.IsNotExist(err) {
		return err
	}

	return ParseBinlogToMessages(binlogFilename, tableMap, consumerChain.consumeMessage)
}

type ConsumerChain struct {
	predicates  []predicate
	collectors  []collector
	prettyPrint bool
}

type predicate func(message Message) bool

type collector func(message Message) error

func NewConsumerChain() ConsumerChain {
	return ConsumerChain{}
}

func (c *ConsumerChain) IncludeTables(tables ...string) {
	c.predicates = append(c.predicates, tablesPredicate(tables...))
}

func (c *ConsumerChain) IncludeSchemas(schemas ...string) {
	c.predicates = append(c.predicates, schemaPredicate(schemas...))
}

func (c *ConsumerChain) PrettyPrint(prettyPrint bool) {
	c.prettyPrint = prettyPrint
}

func (c *ConsumerChain) CollectAsJson(stream io.Writer, prettyPrint bool) {
	c.collectors = append(c.collectors, streamCollector(stream, prettyPrint))
}

func (c *ConsumerChain) consumeMessage(message Message) error {
	for _, predicate := range c.predicates {
		pass := predicate(message)

		if !pass {
			return nil
		}
	}

	for _, collector := range c.collectors {
		collector_err := collector(message)

		if collector_err != nil {
			return collector_err
		}
	}

	return nil
}

func streamCollector(stream io.Writer, prettyPrint bool) collector {
	return func(message Message) error {
		json, err := marshalMessage(message, prettyPrint)

		if err != nil {
			logger.DefaultLogger.Errorx("Failed to convert message to JSON: %s", []interface{}{err})
			return err
		}

		n, err := stream.Write([]byte(fmt.Sprintf("%s\n", json)))

		if err != nil {
			logger.DefaultLogger.Errorx("Failed to write message JSON to file %s", []interface{}{err})
			return err
		}

		logger.DefaultLogger.Infox("Wrote %d bytes to stream", []interface{}{n})

		return nil
	}
}

func schemaPredicate(databases ...string) predicate {
	return func(message Message) bool {
		if message.GetHeader().Schema == "" {
			return true
		}

		return contains(databases, message.GetHeader().Schema)
	}
}

func tablesPredicate(tables ...string) predicate {
	return func(message Message) bool {
		if message.GetHeader().Table == "" {
			return true
		}

		return contains(tables, message.GetHeader().Table)
	}
}

func marshalMessage(message Message, prettyPrint bool) ([]byte, error) {
	if prettyPrint {
		return json.MarshalIndent(message, "", "    ")
	}

	return json.Marshal(message)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
