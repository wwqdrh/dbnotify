package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"
	"time"

	ptypes_struct "github.com/golang/protobuf/ptypes/struct"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	testConnectionString         = "postgres://postgres:hui123456@localhost:5432/datamanager?sslmode=disable"
	testConnectionStringTemplate = "postgres://postgres:hui123456@localhost:5432/%s?sslmode=disable"

	testStreamDatabaseDDL = `create table if not exists notes_stream (id serial, created_at timestamp, name varchar(100), note text)`
	testStreamInsert      = `insert into notes_stream values (default, default, 'user1', 'here is a sample note')`
	// testStreamInsertTemplate  = `insert into notes_steram values (default, default, 'user1', '%s')`
	testStreamDatabaseDropDDL = `drop table notes_stream`
	testStreamUpdate          = `update notes_stream set note = 'here is an updated note' where id=1`
	// testStreamUpdateTemplate  = `update notes_stream set note = 'i%s' where id=1`
)

type StreamSuite struct {
	suite.Suite

	stream *Stream
}

func TestStreamSuite(t *testing.T) {
	tableRe, err := regexp.Compile(".*")
	require.Nil(t, err)

	server, err := NewServer("postgres://postgres:hui123456@127.0.0.1:5432/datamanager?sslmode=disable", []ServerOption{
		WithTableRegexp(tableRe),
	}...)
	require.Nil(t, err)

	suite.Run(t, &StreamSuite{
		stream: server,
	})
}

func (s *StreamSuite) SetupSuite() {
	_, err := s.stream.db.Exec(testStreamDatabaseDDL)
	require.Nil(s.T(), err)
}

func (s *StreamSuite) TearDownSuite() {
	_, err := s.stream.db.Exec(testStreamDatabaseDropDDL)
	require.Nil(s.T(), err)
}

func (s *StreamSuite) TestServerWithSubscribe() {
	timeoutCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	// 模拟channel
	queue := make(chan string, 10)

	// 初始化server
	go func(c context.Context) {
		if err := errors.Wrap(s.stream.RemoveTriggers(), "RemoveTriggers"); err != nil {
			s.T().Error(err)
		}
		if err := s.stream.InstallTriggers(); err != nil {
			s.T().Error(err)
		}

		go func() {
			assert.Nil(s.T(), s.stream.HandleEvents(c, queue))
		}()

		for {
			select {
			case <-c.Done():
				fmt.Println("退出1")
				return
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}(timeoutCtx)

	// 初始化监听者
	go func(c context.Context) {
		for {
			select {
			case <-c.Done():
				fmt.Println("退出2")
				return
			case data := <-queue:
				fmt.Println(data)
			}
		}
	}(timeoutCtx)

	time.Sleep(3 * time.Second)
}

// Test Trigger Install and Uninstall
func (s *StreamSuite) TestServerTriggers() {
	tests := []struct {
		name           string
		re             string
		nTimes         int
		wantInstallErr bool
		wantRemoveErr  bool
	}{
		{"basic", ".*", 1, false, false},
		{"basic_nomatch", "nomatch", 1, true, false},
		{"basic_drop", ".*", 2, false, false},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			cs, cleanup := testDBConn(t, s.stream.db, tt.name)
			defer cleanup()
			s, err := NewServer(cs, WithTableRegexp(regexp.MustCompile(tt.re)))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err = s.InstallTriggers(); (err != nil) != tt.wantInstallErr {
				t.Errorf("Server.InstallTriggers() error = %v, wantErr %v", err, tt.wantInstallErr)
				return
			}
			for i := 0; i < tt.nTimes; i++ {
				t.Log(i)
				err = s.RemoveTriggers()
				t.Log("remove:", err)
				if i == tt.nTimes-1 && (err != nil) != tt.wantRemoveErr {
					t.Errorf("Server.RemoveTriggers() error = %v, wantErr %v", err, tt.wantRemoveErr)
				}
			}
		})
	}
}

func (s *StreamSuite) TestHandleEvents() {
	type testCase struct {
		name    string
		fn      func(*testing.T, *Stream)
		wantErr bool
	}
	tests := []testCase{
		{"basics", nil, false},
		{"basic_insert", func(t *testing.T, s *Stream) {
			if _, err := s.db.Exec(testStreamInsert); err != nil {
				t.Fatal(err)
			}
		}, false},
		{"basic_insert_and_update", func(t *testing.T, s *Stream) {
			if _, err := s.db.Exec(testStreamInsert); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
			if _, err := s.db.Exec(testStreamUpdate); err != nil {
				t.Fatal(err)
			}
		}, false},
	}

	for _, tt := range tests {
		tt := tt
		s.T().Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			cs, cleanup := testDBConn(t, s.stream.db, tt.name)
			defer cleanup()
			stream, err := NewServer(cs)
			stream.listenerPingInterval = time.Second // move into a helper?
			if err != nil {
				t.Fatal(err)
			}

			_, err = stream.db.Exec(testStreamDatabaseDDL)
			if err != nil {
				t.Fatal(err)
			}
			stream.InstallTriggers()

			defer func() {
				if err := stream.Close(); err != nil {
					t.Error(err)
				}
			}()
			go func(t *testing.T, tt testCase) {
				assert.Nil(t, stream.HandleEvents(ctx, nil))
			}(t, tt)
			if tt.fn != nil {
				tt.fn(t, stream)
			}
			if err := stream.RemoveTriggers(); err != nil {
				t.Error(err)
			}
			<-ctx.Done()
		})
	}
}

func Test_generatePatch(t *testing.T) {
	type args struct {
		a *ptypes_struct.Struct
		b *ptypes_struct.Struct
	}
	tests := []struct {
		name     string
		args     args
		wantJSON string
		wantErr  bool
	}{
		{"nils", args{nil, nil}, "{}", false},
		{"empties", args{&ptypes_struct.Struct{}, &ptypes_struct.Struct{}}, "{}", false},
		{"basic", args{&ptypes_struct.Struct{}, &ptypes_struct.Struct{
			Fields: map[string]*ptypes_struct.Value{
				"foo": {
					Kind: &ptypes_struct.Value_StringValue{
						StringValue: "bar",
					},
				},
			},
		}}, `{"foo":"bar"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generatePatch(tt.args.a, tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("generatePatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotJSONByte, err := protojson.Marshal(got)
			require.Nil(t, err)
			getJSON := string(gotJSONByte)

			if !cmp.Equal(getJSON, tt.wantJSON) {
				t.Errorf("generatePatch() = %v, want %v\n%s", getJSON, tt.wantJSON, cmp.Diff(getJSON, tt.wantJSON))
			}
		})
	}
}

func TestServer_redactFields(t *testing.T) {

	rfields := FieldRedactions{
		"public": {"users": []string{
			"password",
			"email",
		},
		},
	}

	s, err := NewServer(testConnectionString, WithFieldRedactions(rfields))
	if err != nil {
		t.Fatal(err)
	}

	event := &RawEvent{
		Schema: "public",
		Table:  "users",
		Payload: &ptypes_struct.Struct{
			Fields: map[string]*ptypes_struct.Value{
				"first_name": {
					Kind: &ptypes_struct.Value_StringValue{StringValue: "first_name"},
				},
				"last_name": {
					Kind: &ptypes_struct.Value_StringValue{StringValue: "last_name"},
				},
				"password": {
					Kind: &ptypes_struct.Value_StringValue{StringValue: "_insecure_"},
				},
				"email": {
					Kind: &ptypes_struct.Value_StringValue{StringValue: "someone@corp.com"},
				},
			},
		},
	}

	type args struct {
		redactions FieldRedactions
		incoming   *RawEvent
		expected   *RawEvent
	}
	tests := []struct {
		name string
		args args
	}{
		{"nil", args{redactions: rfields, incoming: nil}},
		{"nil_payload", args{redactions: rfields, incoming: &RawEvent{}}},
		{"nil_payload_matching", args{redactions: rfields, incoming: &RawEvent{
			Schema: "public",
			Table:  "users",
		}}},
		{"nil_payload_nonnil_previous", args{redactions: rfields, incoming: &RawEvent{
			Schema: "public",
			Table:  "users",
			Previous: &ptypes_struct.Struct{
				Fields: map[string]*ptypes_struct.Value{
					"password": {
						Kind: &ptypes_struct.Value_StringValue{StringValue: "password"},
					},
				},
			},
		}}},
		{
			name: "found",
			args: args{
				redactions: FieldRedactions{
					"public": {"users": []string{
						"first_name",
						"last_name",
					},
					},
				},
				incoming: event,
				expected: &RawEvent{
					Schema: "public",
					Table:  "users",
					Payload: &ptypes_struct.Struct{
						Fields: map[string]*ptypes_struct.Value{
							"first_name": {
								Kind: &ptypes_struct.Value_StringValue{StringValue: "first_name"},
							},
							"last_name": {
								Kind: &ptypes_struct.Value_StringValue{StringValue: "last_name"},
							},
						},
					},
				},
			},
		},
		{
			name: "not_found",
			args: args{
				redactions: rfields,
				incoming:   event,
				expected:   event,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s.redactions = tt.args.redactions
			s.redactFields(tt.args.incoming)

			fmt.Println(tt.args.incoming, tt.args.expected)
			// if got := tt.args.incoming; tt.args.expected != nil && !cmp.Equal(got, tt.args.expected) {
			// 	t.Errorf("s.redactFields()= %v, want %v", got, tt.args.expected)
			// }
		})
	}
}

func TestDecodeRedactions(t *testing.T) {
	type args struct {
		r string
	}
	tests := []struct {
		name    string
		args    args
		want    FieldRedactions
		wantErr bool
	}{
		{
			name: "basic",
			args: args{r: `{"public":{"users":["first_name","last_name","email"]}}`},
			want: FieldRedactions{
				"public": {"users": []string{
					"first_name",
					"last_name",
					"email",
				},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeRedactions(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeRedactions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("DecodeRedactions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWithTableRegexp(t *testing.T) {
	re := regexp.MustCompile(".*")
	tests := []struct {
		name string
		want *regexp.Regexp
	}{
		{"basic", re},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewServer(testConnectionString, WithTableRegexp(re))
			if err != nil {
				t.Fatal(err)
			}
			if got := s.tableRe; got != tt.want {
				t.Errorf("WithTableRegexp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewServer(t *testing.T) {
	type args struct {
		connectionString string
		opts             []ServerOption
	}
	tests := []struct {
		name    string
		args    args
		check   func(t *testing.T, s *Stream)
		wantErr bool
	}{
		{"bad", args{
			connectionString: "this is an invalid connection string",
		}, nil, true},
		{"empty", args{
			connectionString: "",
		}, nil, true},
		{"good", args{
			connectionString: testConnectionString,
		}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewServer(tt.args.connectionString, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func testDBConn(t *testing.T, db *sql.DB, testcase string) (connectionString string, cleanup func()) {
	s := fmt.Sprintf("test_pqstream_%s", testcase)
	db.Exec(fmt.Sprintf("drop database %s", s))
	_, err := db.Exec(fmt.Sprintf("create database %s", s))
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf(testConnectionStringTemplate, s)
	newDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skip(err)
	}
	if err := db.Ping(); err != nil {
		t.Skip(errors.Wrap(err, "ping"))
	}
	defer newDB.Close()
	_, err = newDB.Exec(testStreamDatabaseDDL)
	if err != nil {
		t.Fatal(err)
	}
	return dsn, func() {
		_, err := db.Exec(fmt.Sprintf("drop database %s", s))
		if err != nil {
			t.Fatal(err)
		}
	}
}
