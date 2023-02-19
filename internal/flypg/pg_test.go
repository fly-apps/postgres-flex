package flypg

import (
	"fmt"
	"os"
	"testing"
)

const (
	pgTestDirectory          = "./test_results"
	pgConfigFilePath         = "./test_results/postgresql.conf"
	pgInternalConfigFilePath = "./test_results/postgresql.internal.conf"
	pgUserConfigFilePath     = "./test_results/postgresql.user.conf"
)

func TestPGConfigDefaults(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		dataDir:                pgTestDirectory,
		port:                   5433,
		configFilePath:         pgConfigFilePath,
		internalConfigFilePath: pgInternalConfigFilePath,
		userConfigFilePath:     pgUserConfigFilePath,
		userConfig:             ConfigMap{},
		internalConfig:         ConfigMap{},
	}

	if err := pgConf.initialize(); err != nil {
		t.Error(err)
	}

	cfg, err := ReadFromFile(pgInternalConfigFilePath)
	if err != nil {
		t.Error(err)
	}

	if cfg["port"] != "5433" {
		t.Fatalf("expected port to be 5433, got %v", cfg["port"])
	}

	if cfg["hot_standby"] != "true" {
		t.Fatalf("expected hot_standby to be true, got %v", cfg["hot_standby"])
	}
}

func TestPGSettingOverride(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		dataDir:                pgTestDirectory,
		port:                   5433,
		configFilePath:         pgConfigFilePath,
		internalConfigFilePath: pgInternalConfigFilePath,
		userConfigFilePath:     pgUserConfigFilePath,
		userConfig: ConfigMap{
			"log_statement": "ddl",
			"port":          "10000",
		},
		internalConfig: ConfigMap{},
	}

	if err := pgConf.initialize(); err != nil {
		t.Error(err)
	}

	if err := WriteConfigFiles(pgConf); err != nil {
		t.Error(err)
	}

	cfg, err := pgConf.CurrentConfig()
	if err != nil {
		t.Fatal(err)
	}

	if cfg["port"] != "10000" {
		t.Fatalf("expected port to be 10000, got %v", cfg["port"])
	}

	if cfg["log_statement"] != "ddl" {
		t.Fatalf("expected log_statement to be ddl, got %v", cfg["log_statement"])
	}

}

func setup(t *testing.T) error {
	t.Setenv("FLY_VM_MEMORY_MB", fmt.Sprint(256*(1024*1024)))
	t.Setenv("UNIT_TESTING", "true")

	if _, err := os.Stat(pgTestDirectory); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(pgTestDirectory, 0750); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	file, err := os.Create(pgConfigFilePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return file.Sync()

}

func cleanup() {
	os.RemoveAll(pgTestDirectory)
}

// type mockAPI struct {
// 	ts *httptest.Server
// 	t  *testing.T
// 	mock.Mock
// }

// func setupMockAPI(t *testing.T) (*mockAPI, *store.Store) {
// 	mapi := mockAPI{t: t}
// 	mapi.Test(t)

// 	mapi.ts = httptest.NewServer(&mapi)
// 	t.Cleanup(func() {
// 		mapi.ts.Close()
// 		mapi.Mock.AssertExpectations(t)
// 	})

// 	cfg := api.DefaultConfig()
// 	cfg.Address = mapi.ts.URL

// 	client, err := api.NewClient(cfg)
// 	require.NoError(t, err)

// 	store, err := store.NewStore()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	store.Client = client
// 	return &mapi, store
// }

// func (m *mockAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
// 	var body interface{}

// 	if r.Body != nil {
// 		bodyBytes, err := io.ReadAll(r.Body)
// 		if err == nil && len(bodyBytes) > 0 {
// 			body = bodyBytes

// 			var bodyMap map[string]interface{}
// 			if err := json.Unmarshal(bodyBytes, &bodyMap); err != nil {
// 				body = bodyMap
// 			}
// 		}
// 	}

// 	ret := m.Called(r.Method, r.URL.Path, body)

// 	if replyFn, ok := ret.Get(0).(func(http.ResponseWriter, *http.Request)); ok {
// 		replyFn(w, r)
// 		return
// 	}
// }

// func (m *mockAPI) static(method string, path string, body interface{}) *mock.Call {
// 	return m.On("ServeHTTP", method, path, body)
// }

// func (m *mockAPI) withReply(method, path string, body interface{}, status int, reply interface{}) *mock.Call {
// 	return m.static(method, path, body).Return(func(w http.ResponseWriter, r *http.Request) {
// 		w.WriteHeader(status)

// 		if reply == nil {
// 			return
// 		}

// 		rdr, ok := reply.(io.Reader)
// 		if ok {
// 			io.Copy(w, rdr)
// 			return
// 		}

// 		enc := json.NewEncoder(w)
// 		require.NoError(m.t, enc.Encode(reply))
// 	})
// }
