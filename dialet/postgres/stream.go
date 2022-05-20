package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/golang/protobuf/jsonpb"

	"bytes"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	jsonpatch "github.com/evanphx/json-patch"

	ptypes_struct "github.com/golang/protobuf/ptypes/struct"
)

const (
	minReconnectInterval = time.Second
	maxReconnectInterval = 10 * time.Second
	defaultPingInterval  = 9 * time.Second
	channel              = "pqstream_notify"

	fallbackIDColumnType = "integer" // TODO(tmc) parameterize
)

type Stream struct {
	l   *pq.Listener
	db  *sql.DB
	ctx context.Context

	tableRe *regexp.Regexp

	listenerPingInterval time.Duration
	// subscribe            chan *subscription
	redactions FieldRedactions
}

type ServerOption func(*Stream)

// WithTableRegexp controls which tables are managed.
func WithTableRegexp(re *regexp.Regexp) ServerOption {
	return func(s *Stream) {
		s.tableRe = re
	}
}

// WithContext allows supplying a custom context.
func WithContext(ctx context.Context) ServerOption {
	return func(s *Stream) {
		s.ctx = ctx
	}
}

func generatePatch(a, b *ptypes_struct.Struct) (*ptypes_struct.Struct, error) {
	abytes := &bytes.Buffer{}
	bbytes := &bytes.Buffer{}
	m := &jsonpb.Marshaler{}

	if a != nil {
		if err := m.Marshal(abytes, a); err != nil {
			return nil, err
		}
	}
	if b != nil {
		if err := m.Marshal(bbytes, b); err != nil {
			return nil, err
		}
	}
	if abytes.Len() == 0 {
		abytes.Write([]byte("{}"))
	}
	if bbytes.Len() == 0 {
		bbytes.Write([]byte("{}"))
	}
	p, err := jsonpatch.CreateMergePatch(abytes.Bytes(), bbytes.Bytes())
	if err != nil {
		return nil, err
	}
	r := &ptypes_struct.Struct{}
	rbytes := bytes.NewReader(p)
	err = (&jsonpb.Unmarshaler{}).Unmarshal(rbytes, r)
	return r, err
}

// FieldRedactions describes how redaction fields are specified.
// Top level map key is the schema, inner map key is the table and slice is the fields to redact.
type FieldRedactions map[string]map[string][]string

// DecodeRedactions returns a FieldRedactions map decoded from redactions specified in json format.
func DecodeRedactions(r string) (FieldRedactions, error) {
	rfields := make(FieldRedactions)
	if err := json.NewDecoder(strings.NewReader(r)).Decode(&rfields); err != nil {
		return nil, err
	}

	return rfields, nil
}

// WithFieldRedactions controls which fields are redacted from the feed.
func WithFieldRedactions(r FieldRedactions) ServerOption {
	return func(s *Stream) {
		s.redactions = r
	}
}

// redactFields search through redactionMap if there's any redacted fields
// specified that match the fields of the current event.
func (s *Stream) redactFields(e *RawEvent) {
	if tables, ok := s.redactions[e.GetSchema()]; ok {
		if fields, ok := tables[e.GetTable()]; ok {
			for _, rf := range fields {
				if e.Payload != nil {
					if _, ok := e.Payload.Fields[rf]; ok {
						//remove field from payload
						delete(e.Payload.Fields, rf)
					}
				}
				if e.Previous != nil {
					if _, ok := e.Previous.Fields[rf]; ok {
						//remove field from previous payload
						delete(e.Previous.Fields, rf)
					}
				}
			}
		}
	}
}

// NewServer prepares a new pqstream server.
func NewServer(connectionString string, opts ...ServerOption) (*Stream, error) {
	s := &Stream{
		redactions:           make(FieldRedactions),
		ctx:                  context.Background(),
		listenerPingInterval: defaultPingInterval,
	}
	for _, o := range opts {
		o(s)
	}
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Wrap(err, "ping")
	}
	s.l = pq.NewListener(connectionString, minReconnectInterval, maxReconnectInterval, func(ev pq.ListenerEventType, err error) {
		fmt.Printf("listener-event %v, got listener event\n", ev)
		if err != nil {
			fmt.Println(err.Error() + "got listener event error")
		}
	})
	if err := s.l.Listen(channel); err != nil {
		return nil, errors.Wrap(err, "listen")
	}
	if err := s.l.Listen(channel + "-ctl"); err != nil {
		return nil, errors.Wrap(err, "listen")
	}
	s.db = db
	return s, nil
}

// Close stops the pqstream server.
func (s *Stream) Close() error {
	errL := s.l.Close()
	errDB := s.db.Close()
	if errL != nil {
		return errors.Wrap(errL, "listener")
	}
	if errDB != nil {
		return errors.Wrap(errDB, "DB")
	}
	return nil
}

// InstallTriggers sets up triggers to start observing changes for the set of tables in the database.
func (s *Stream) InstallTriggers() error {
	_, err := s.db.Exec(sqlTriggerFunction)
	if err != nil {
		return err
	}
	// TODO(tmc): watch for new tables
	tableNames, err := s.tableNames()
	if err != nil {
		return err
	}
	for _, t := range tableNames {
		if err := s.installTrigger(t); err != nil {
			return errors.Wrap(err, fmt.Sprintf("installTrigger table %s", t))
		}
	}
	if len(tableNames) == 0 {
		return errors.New("no tables found")
	}
	return nil
}

func (s *Stream) tableNames() ([]string, error) {
	rows, err := s.db.Query(sqlQueryTables)
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintln("tableNames scan, after", len(tableNames)))
		}
		if s.tableRe != nil && !s.tableRe.MatchString(t) {
			continue
		}
		tableNames = append(tableNames, t)
	}
	return tableNames, nil
}

func (s *Stream) installTrigger(table string) error {
	q := fmt.Sprintf(sqlInstallTrigger, table)
	_, err := s.db.Exec(q)
	return err
}

// RemoveTriggers removes triggers from the database.
func (s *Stream) RemoveTriggers() error {
	tableNames, err := s.tableNames()
	if err != nil {
		return err
	}
	for _, t := range tableNames {
		if err := s.removeTrigger(t); err != nil {
			return errors.Wrap(err, fmt.Sprintf("removeTrigger table:%s", t))
		}
	}
	return nil
}

func (s *Stream) removeTrigger(table string) error {
	q := fmt.Sprintf(sqlRemoveTrigger, table)
	_, err := s.db.Exec(q)
	return err
}

// fallbackLookup will be invoked if we have apparently exceeded the 8000 byte notify limit.
func (s *Stream) fallbackLookup(e *Event) error {
	rows, err := s.db.Query(fmt.Sprintf(sqlFetchRowByID, e.Table, fallbackIDColumnType), e.Id)
	if err != nil {
		return errors.Wrap(err, "fallback query")
	}
	defer rows.Close()
	if rows.Next() {
		payload := ""
		if err := rows.Scan(&payload); err != nil {
			return errors.Wrap(err, "fallback scan")
		}
		e.Payload = &ptypes_struct.Struct{}
		if err := jsonpb.UnmarshalString(payload, e.Payload); err != nil {
			return errors.Wrap(err, "fallback unmarshal")
		}
	}
	return nil
}

func (s *Stream) handleEvent(ev *pq.Notification, q chan string) error {
	if ev == nil {
		return errors.New("got nil event")
	}

	re := &RawEvent{}
	if err := jsonpb.UnmarshalString(ev.Extra, re); err != nil {
		return errors.Wrap(err, "jsonpb unmarshal")
	}

	// perform field redactions
	s.redactFields(re)

	e := &Event{
		Schema:  re.Schema,
		Table:   re.Table,
		Op:      re.Op,
		Id:      re.Id,
		Payload: re.Payload,
	}

	if re.Op == Operation_UPDATE {
		if patch, err := generatePatch(re.Payload, re.Previous); err != nil {
			fmt.Println("event " + err.Error() + "issue generating json patch")
		} else {
			e.Changes = patch
		}
	}

	if e.Payload == nil && e.Id != "" {
		if err := s.fallbackLookup(e); err != nil {
			fmt.Println("event " + err.Error() + "fallback lookup failed")
		}

	}
	if q == nil {
		return nil
	}

	data, err := json.Marshal(e)
	if err == nil {
		q <- string(data)
	}
	return nil
}

// HandleEvents processes events from the database and copies them to relevant clients.
func (s *Stream) HandleEvents(ctx context.Context, q chan string) error {
	// subscribers := map[*subscription]bool{}
	events := s.l.NotificationChannel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-events:
			// TODO(tmc): separate case handling into method
			fmt.Printf("event %v fallback lookup failed", ev)
			if err := s.handleEvent(ev, q); err != nil {
				return err
			}
		case <-time.After(s.listenerPingInterval):
			fmt.Println("interval " + s.listenerPingInterval.String() + "pinging")
			if err := s.l.Ping(); err != nil {
				return errors.Wrap(err, "Ping")
			}
		}
	}
}
