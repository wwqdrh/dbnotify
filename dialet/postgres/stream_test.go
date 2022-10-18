package postgres

import (
	"context"
	"fmt"
	"os"
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
	testStreamDatabaseDDL = `create table if not exists notes_stream (id serial, created_at timestamp, name varchar(100), note text)`
	// testStreamInsert      = `insert into notes_stream values (default, default, 'user1', 'here is a sample note')`
	// testStreamInsertTemplate  = `insert into notes_steram values (default, default, 'user1', '%s')`
	testStreamDatabaseDropDDL = `drop table notes_stream`
	// testStreamUpdate          = `update notes_stream set note = 'here is an updated note' where id=1`
	// testStreamUpdateTemplate  = `update notes_stream set note = 'i%s' where id=1`
)

type StreamSuite struct {
	suite.Suite

	stream *Stream
}

func TestStreamSuite(t *testing.T) {
	dsn := os.Getenv("POSTGRES")
	if dsn == "" {
		t.Skip("POSTGRES为空")
	}

	tableRe, err := regexp.Compile(".*")
	require.Nil(t, err)

	server, err := NewServer(dsn, []ServerOption{
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

func (s *StreamSuite) TestGeneratePatch() {
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
		s.T().Run(tt.name, func(t *testing.T) {
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

func (s *StreamSuite) TestServerRedactFields() {

	rfields := FieldRedactions{
		"public": {"users": []string{
			"password",
			"email",
		},
		},
	}

	srv, err := NewServer(os.Getenv("POSTGRES"), WithFieldRedactions(rfields))
	if err != nil {
		s.T().Fatal(err)
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
		s.T().Run(tt.name, func(t *testing.T) {
			srv.redactions = tt.args.redactions
			srv.redactFields(tt.args.incoming)

			fmt.Println(tt.args.incoming, tt.args.expected)
			// if got := tt.args.incoming; tt.args.expected != nil && !cmp.Equal(got, tt.args.expected) {
			// 	t.Errorf("s.redactFields()= %v, want %v", got, tt.args.expected)
			// }
		})
	}
}

func (s *StreamSuite) TestDecodeRedactions() {
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
		s.T().Run(tt.name, func(t *testing.T) {
			got, err := DecodeRedactions(tt.args.r)
			if (err != nil) != tt.wantErr {
				s.T().Errorf("DecodeRedactions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want) {
				s.T().Errorf("DecodeRedactions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func (s *StreamSuite) TestWithTableRegexp() {
	re := regexp.MustCompile(".*")
	tests := []struct {
		name string
		want *regexp.Regexp
	}{
		{"basic", re},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			s, err := NewServer(os.Getenv("POSTGRES"), WithTableRegexp(re))
			if err != nil {
				t.Fatal(err)
			}
			if got := s.tableRe; got != tt.want {
				t.Errorf("WithTableRegexp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func (s *StreamSuite) TestNewServer() {
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
			connectionString: os.Getenv("POSTGRES"),
		}, nil, false},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
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
