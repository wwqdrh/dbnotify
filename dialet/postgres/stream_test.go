package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	ptypes_struct "github.com/golang/protobuf/ptypes/struct"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

var testConnectionString = "postgres://localhost?sslmode=disable"
var testConnectionStringTemplate = "postgres://localhost/%s?sslmode=disable"

// var (
// 	testDatabaseDDL    = `create table notes (id serial, created_at timestamp, name varchar(100), note text)`
// 	testInsert         = `insert into notes values (default, default, 'user1', 'here is a sample note')`
// 	testInsertTemplate = `insert into notes values (default, default, 'user1' '%s')`
// 	testUpdate         = `update notes set note = 'here is an updated note' where id=1`
// 	testUpdateTemplate = `update notes set note = 'i%s' where id=1`
// )

func init() {
	if s := os.Getenv("PQSTREAM_TEST_DB_URL"); s != "" {
		testConnectionString = s
	}
	if s := os.Getenv("PQSTREAM_TEST_DB_TMPL_URL"); s != "" {
		testConnectionStringTemplate = s
	}
}

func TestMain(m *testing.M) {
	if os.Getenv("LOCAL") == "" {
		return
	}

	m.Run()
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
			gotJSON, err := (&jsonpb.Marshaler{}).MarshalToString(got)
			if err != nil {
				t.Error(err)
			}
			if !cmp.Equal(gotJSON, tt.wantJSON) {
				t.Errorf("generatePatch() = %v, want %v\n%s", gotJSON, tt.wantJSON, cmp.Diff(gotJSON, tt.wantJSON))
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

func TestServerWithSubscribe(t *testing.T) {
	timeoutCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	// 模拟channel
	queue := make(chan string, 10)

	// 初始化server
	go func(c context.Context) {
		tableRe, err := regexp.Compile(".*")
		if err != nil {
			return
		}

		opts := []ServerOption{
			WithTableRegexp(tableRe),
		}

		server, err := NewServer("postgres://postgres:hui123456@127.0.0.1:5432/datamanager?sslmode=disable", opts...)
		if err != nil {
			t.Error(err)
			return
		}

		if err = errors.Wrap(server.RemoveTriggers(), "RemoveTriggers"); err != nil {
			t.Error(err)
		}
		if err = server.InstallTriggers(); err != nil {
			t.Error(err)
		}

		go func() {
			if err = server.HandleEvents(c, queue); err != nil {
				// TODO(tmc): try to be more graceful
				log.Fatalln(err)
			}
		}()

		for {
			select {
			case <-c.Done():
				fmt.Println("退出1")
				return
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

	time.Sleep(30 * time.Second)
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

func dbOrSkip(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", testConnectionString)
	if err != nil {
		t.Skip(err)
	}
	if err := db.Ping(); err != nil {
		t.Skip(errors.Wrap(err, "ping"))
	}
	return db
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
	_, err = newDB.Exec(testDatabaseDDL)
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

func mkString(len int, c byte) string {
	buf := make([]byte, len)
	for i := range buf {
		buf[i] = c
	}
	return string(buf)
}

func TestServer_HandleEvents(t *testing.T) {
	db := dbOrSkip(t)
	type testCase struct {
		name    string
		fn      func(*testing.T, *Stream)
		wantErr bool
	}
	tests := []testCase{
		{"basics", nil, false},
		{"basic_insert", func(t *testing.T, s *Stream) {
			if _, err := s.db.Exec(testInsert); err != nil {
				t.Fatal(err)
			}
		}, false},
		{"basic_insert_and_update", func(t *testing.T, s *Stream) {
			if _, err := s.db.Exec(testInsert); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
			if _, err := s.db.Exec(testUpdate); err != nil {
				t.Fatal(err)
			}
		}, false},
	}

	mkTestCase := func(n int, alsoUpdate bool) testCase {
		caseName := fmt.Sprintf("test_%vb_insert", n)
		if alsoUpdate {
			caseName += "_and_update"
		}
		return testCase{caseName, func(t *testing.T, s *Stream) {
			insert := fmt.Sprintf(testInsertTemplate, mkString(n, '.'))
			fmt.Printf("inseting %d \n", n)
			if _, err := s.db.Exec(insert); err != nil {
				t.Fatal(err)
			}
			if alsoUpdate {
				time.Sleep(10 % time.Millisecond)
				update := fmt.Sprintf(testUpdateTemplate, mkString(n, '-'))
				if _, err := s.db.Exec(update); err != nil {
					t.Fatal(err)
				}
			}
		}, false}
	}

	// TODO(tmc): encode the expected properties of the payloads in test
	// cross the 8k boundary for inserts
	for i := 7870; i <= 7900; i = i + 10 {
		tests = append(tests, mkTestCase(i, false))
	}
	// cross the 8k boundary for updates (and drop previous payloads)
	for i := 3890; i <= 4000; i = i + 10 {
		tests = append(tests, mkTestCase(i, true))
	}
	// cross the 8k boundary for updates (and drop payloads)
	for i := 7870; i <= 7900; i = i + 10 {
		tests = append(tests, mkTestCase(i, true))
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			cs, cleanup := testDBConn(t, db, tt.name)
			defer cleanup()
			s, err := NewServer(cs)
			s.listenerPingInterval = time.Second // move into a helper?
			if err != nil {
				t.Fatal(err)
			}
			s.InstallTriggers()
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			go func(t *testing.T, tt testCase) {
				if err := s.HandleEvents(ctx, nil); (err != nil) != tt.wantErr {
					t.Errorf("Server.HandleEvents(%v) error = %v, wantErr %v", ctx, err, tt.wantErr)
				}
			}(t, tt)
			if tt.fn != nil {
				tt.fn(t, s)
			}
			if err := s.RemoveTriggers(); err != nil {
				t.Error(err)
			}
			<-ctx.Done()

		})
	}
}

// Test Trigger Install and Uninstall
func TestServer_Triggers(t *testing.T) {
	db := dbOrSkip(t)
	tests := []struct {
		name           string
		re             string
		nTimes         int
		dropBetween    bool
		wantInstallErr bool
		wantRemoveErr  bool
	}{
		{"basic", ".*", 1, false, false, false},
		{"basic_nomatch", "nomatch", 1, false, true, false},
		{"basic_drop", ".*", 2, true, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs, cleanup := testDBConn(t, db, tt.name)
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
				if tt.dropBetween && i > 0 {
					_, err := s.db.Exec("drop table notes")
					if err != nil {
						t.Log(err)
					}
				}
				err = s.RemoveTriggers()
				t.Log("remove:", err)
				if i == tt.nTimes-1 && (err != nil) != tt.wantRemoveErr {
					t.Errorf("Server.RemoveTriggers() error = %v, wantErr %v", err, tt.wantRemoveErr)
				}
			}
		})
	}
}
