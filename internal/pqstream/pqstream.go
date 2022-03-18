package pqstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/sirupsen/logrus"
	"github.com/wwqdrh/datamanager/internal/pqstream/proto"

	"github.com/lib/pq"
	"github.com/pkg/errors"

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
	logger logrus.FieldLogger
	l      *pq.Listener
	db     *sql.DB
	ctx    context.Context

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

// WithLogger allows attaching a custom logger.
func WithLogger(l logrus.FieldLogger) ServerOption {
	return func(s *Stream) {
		s.logger = l
	}
}

// WithContext allows supplying a custom context.
func WithContext(ctx context.Context) ServerOption {
	return func(s *Stream) {
		s.ctx = ctx
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
	if s.logger == nil {
		s.logger = logrus.StandardLogger()
	}
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Wrap(err, "ping")
	}
	s.l = pq.NewListener(connectionString, minReconnectInterval, maxReconnectInterval, func(ev pq.ListenerEventType, err error) {
		s.logger.WithField("listener-event", ev).Debugln("got listener event")
		if err != nil {
			s.logger.WithField("listener-event", ev).WithError(err).Errorln("got listener event error")
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
func (s *Stream) fallbackLookup(e *proto.Event) error {
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

	re := &proto.RawEvent{}
	if err := jsonpb.UnmarshalString(ev.Extra, re); err != nil {
		return errors.Wrap(err, "jsonpb unmarshal")
	}

	// perform field redactions
	s.redactFields(re)

	e := &proto.Event{
		Schema:  re.Schema,
		Table:   re.Table,
		Op:      re.Op,
		Id:      re.Id,
		Payload: re.Payload,
	}

	if re.Op == proto.Operation_UPDATE {
		if patch, err := generatePatch(re.Payload, re.Previous); err != nil {
			s.logger.WithField("event", e).WithError(err).Infoln("issue generating json patch")
		} else {
			e.Changes = patch
		}
	}

	if e.Payload == nil && e.Id != "" {
		if err := s.fallbackLookup(e); err != nil {
			s.logger.WithField("event", e).WithError(err).Errorln("fallback lookup failed")
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
			s.logger.WithField("event", ev).Debugln("got event")
			if err := s.handleEvent(ev, q); err != nil {
				return err
			}
		case <-time.After(s.listenerPingInterval):
			s.logger.WithField("interval", s.listenerPingInterval).Debugln("pinging")
			if err := s.l.Ping(); err != nil {
				return errors.Wrap(err, "Ping")
			}
		}
	}
}
