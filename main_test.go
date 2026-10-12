package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMemoryStoreTransition(t *testing.T) {
	memStore := NewMemoryStore()

	var err error

	err = memStore.Add(Job{ID: 1, URL: "https://go.dev", Status: StatusQueued})

	if (err != nil) {
		t.Fatal(err)
	}
	
	err = memStore.Transition(1, StatusRunning)

	if (err != nil) {
		t.Fatal(err)
	}
	
	
	job, err := memStore.Get(1)

	if (err != nil) {
		t.Fatalf("getting job failed: %v", err)
	}

	if job.Status != StatusRunning {
		t.Fatalf("status = %q, wanted: %q", job.Status, StatusRunning)
	}
}

func TestMemoryStoreRejectsInvalidTransition(t *testing.T) {
	memStore := NewMemoryStore()

	var err error

	err = memStore.Add(Job{ID: 1, URL: "https://go.dev", Status: StatusQueued})

	if (err != nil) {
		t.Fatal(err)
	}

	err = memStore.Transition(1, StatusFailed)

	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("%t", errors.Is(err, ErrInvalidTransition))
	}

	j, err := memStore.Get(1)

	if(err != nil) {
		t.Fatalf("getting job failed: %v", err)
	}

	if j.Status != StatusQueued {
		t.Fatalf("wanted %q, found %q", StatusQueued, j.Status)
	}
}

func TestAPIGetJob(t *testing.T) {
	memStore := NewMemoryStore()

	err := memStore.Add(Job{
		ID: 1,
		URL: "https://go.dev",
		Status: StatusQueued,
	})

	if err != nil {
		t.Fatalf("error adding job: %v", err)
	}

	api := &API{store: memStore}


	req := httptest.NewRequest(http.MethodGet, "/jobs?id=1", nil)
	rec := httptest.NewRecorder()

	api.ServeHTTP(rec, req)

	resp := rec.Result()

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("error occured while fetching job %v", 1)
	}

	if (resp.Header.Get("Content-Type") != "application/json") {
		t.Fatalf("content type not ok")
	}

	var got Job

	decodeErr := json.NewDecoder(resp.Body).Decode(&got)

	if (decodeErr != nil) {
		t.Fatalf("decoding response failed: %v", decodeErr)
	}

	if got.ID != 1 {
		t.Fatalf("id = %d, want %d", got.ID, 1)
	} else if got.URL != "https://go.dev" {
		t.Fatalf("url = %s, want %s", got.URL, "https://go.dev")
	} else if (got.Status != StatusQueued) {
		t.Fatalf("status = %s, want %s", got.Status, StatusQueued)
	}
}

func TestAPIJobNotFound(t *testing.T) {
	api := &API{store: NewMemoryStore()}

	req := httptest.NewRequest(http.MethodGet, "/jobs?id=20", nil)
	rec := httptest.NewRecorder()

	api.ServeHTTP(rec, req)

	resp := rec.Result()

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %v, want %v", resp.StatusCode, http.StatusNotFound)
	}
}



func TestAPIRejectsBadRequests(t *testing.T) {
	cases := []struct{
		name string
		method string
		target string
		wantStatus int
		wantAllow string
	}{{
		name: "missing id",
		method: http.MethodGet,
		target: "/jobs",
		wantStatus: http.StatusBadRequest,
		wantAllow: "",
	},
	{
		name: "invalid id",
		method: http.MethodGet,
		target: "/jobs?id=abc",
		wantStatus: http.StatusBadRequest,
		wantAllow: "",
	},
	{
		name: "unknown path",
		method: http.MethodGet,
		target: "/hello?id=1",
		wantStatus: http.StatusNotFound,
		wantAllow: "",
	},
	{
		name: "wrong method",
		method: http.MethodDelete,
		target: "/jobs?id=1",
		wantStatus: http.StatusMethodNotAllowed,
		wantAllow: "GET, POST",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := &API{store: NewMemoryStore()}

			req := httptest.NewRequest(tc.method, tc.target, nil)
			rec := httptest.NewRecorder()

			api.ServeHTTP(rec, req)

			resp := rec.Result()

			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("got %v, want %v", resp.StatusCode, tc.wantStatus)
			}

			if resp.Header.Get("Allow") != tc.wantAllow {
				t.Fatalf("got %s, want %s", resp.Header.Get("Allow"), tc.wantAllow)
			}
		})
	}
}


type failingStore struct {}

func (fs *failingStore) Get(id uint64) (Job, error) {
	return Job{}, errors.New("storage unavailable!")
}

func (fs *failingStore) Create(address string) (Job, error) {
	return Job{}, errors.New("job creation error!")
}

func TestAPIStoreFailure(t *testing.T) {
	api := &API{store: &failingStore{}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/jobs?id=1", nil)

	api.ServeHTTP(rec, req)

	resp := rec.Result()

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("got %v, want %v", resp.StatusCode, http.StatusInternalServerError)
	}

	gotBody := rec.Body.String()

	if gotBody != "internal server error\n" {
		t.Fatalf("body = %s, want %s", gotBody, "internal server error")
	}
}

func TestMemoryStoreConcurrentTransition(t *testing.T) {
	memStore := NewMemoryStore()

	err := memStore.Add(Job{
		ID: 1,
		URL: "https://go.dev",
		Status: StatusQueued,
	})

	if err != nil {
		t.Fatalf("error adding job: %v", err)
	}

	api := &API{store: memStore}

	results := make(chan error, 10)

	for range 10 {
		go func() {
			results <- memStore.Transition(1, StatusRunning)
		}()
	}

	var s, u int32

	for range 10 {
		e := <-results

		if e == nil {
			s++
		} else if errors.Is(e, ErrInvalidTransition) {
			u++
		} else {
			t.Fatalf("unexpected error: %v", e)
		}
	}

	j, err := api.store.Get(1)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if s != 1 || u != 9 || j.Status != StatusRunning {
		t.Fatalf("got s = %v, u = %v, status = %s; wanted s = %v, u = %v, status = %s", s, u, j.Status, 1, 9, StatusRunning)
	}
}

func TestMemoryStoreCreate(t *testing.T) {
    memStore := NewMemoryStore()

    j1, err := memStore.Create("https://example.com")
	
    if err != nil {
        t.Fatalf("creating first job: %v", err)
    }

    want1 := Job{
        ID:     1,
        URL:    "https://example.com",
        Status: StatusQueued,
    }

    if j1 != want1 {
        t.Fatalf("first job = %+v, want %+v", j1, want1)
    }

    j2, err := memStore.Create("https://google.com")
    if err != nil {
        t.Fatalf("creating second job: %v", err)
    }

    want2 := Job{
        ID:     2,
        URL:    "https://google.com",
        Status: StatusQueued,
    }

    if j2 != want2 {
        t.Fatalf("second job = %+v, want %+v", j2, want2)
    }

    got1, err := memStore.Get(1)
    if err != nil {
        t.Fatalf("getting job 1: %v", err)
    }

    if got1 != j1 {
        t.Fatalf("stored job 1 = %+v, want %+v", got1, j1)
    }

    got2, err := memStore.Get(2)
    if err != nil {
        t.Fatalf("error getting job 2: %v", err)
    }

    if got2 != j2 {
        t.Fatalf("stored job 2 = %+v, want %+v", got2, j2)
    }

    // Adding a higher ID must advance the counter.
    err = memStore.Add(Job{
        ID:     10,
        URL:    "https://netflix.com",
        Status: StatusQueued,
    })
    if err != nil {
        t.Fatalf("adding job 10: %v", err)
    }

    j3, err := memStore.Create("https://facebook.com")
    if err != nil {
        t.Fatalf("creating job after adding ID 10: %v", err)
    }

    if j3.ID != 11 {
        t.Fatalf("id = %d, want 11", j3.ID)
    }

    // Adding a lower ID must preserve the counter.
    err = memStore.Add(Job{
        ID:     9,
        URL:    "https://github.com",
        Status: StatusQueued,
    })
    if err != nil {
        t.Fatalf("adding job 9: %v", err)
    }

    j4, err := memStore.Create("https://facebook.com")
    if err != nil {
        t.Fatalf("creating job after adding ID 9: %v", err)
    }

    if j4.ID != 12 {
        t.Fatalf("id = %d, want 12", j4.ID)
    }
}

func TestAPICreateJob(t *testing.T) {
	memStore := NewMemoryStore()
	api := &API{store: memStore}

	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"url":"https://example.com"}`),
	)

	rec := httptest.NewRecorder()

	api.ServeHTTP(rec, req)

	resp := rec.Result()

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %v, want = %v", resp.StatusCode, http.StatusCreated)
	}

	if (resp.Header.Get("Content-Type") != "application/json") {
		t.Fatalf("content type not ok")
	}

	var got Job

	decodeErr := json.NewDecoder(resp.Body).Decode(&got)

	if (decodeErr != nil) {
		t.Fatalf("decoding response failed: %v", decodeErr)
	}

	want := Job{ID: 1, URL: "https://example.com", Status: StatusQueued}

	if got != want {
		t.Fatalf("job = %+v, want = %+v", got, want)
	}

	got, err := memStore.Get(1)

	if err != nil {
		t.Fatalf("error getting job: %v", err)
	}

	if got != want {
		t.Fatalf("job = %+v, want = %+v", got, want)
	}
}

func TestAPIRejectsBadCreateJobRequests(t *testing.T) {
	cases := []struct{
		name string
		method string
		target string
		wantStatus int
		body string
	}{
		{
			name: "Malformed JSON",
			method: http.MethodPost,
			target: "/jobs",
			wantStatus: http.StatusBadRequest,
			body: "{",
		},
		{
			name: "Missing URL Field",
			method: http.MethodPost,
			target: "/jobs",
			wantStatus: http.StatusBadRequest,
			body: "{}",
		},
		{
			name: "Empty URL",
			method: http.MethodPost,
			target: "/jobs",
			wantStatus: http.StatusBadRequest,
			body: "{\"url\":\"\"}",
		},
		{
			name: "Client Supplies ID",
			method: http.MethodPost,
			target: "/jobs",
			wantStatus: http.StatusBadRequest,
			body: `{"url":"https://example.com","id":99}`,
		},
		{
			name: "Client Supplies Status",
			method: http.MethodPost,
			target: "/jobs",
			wantStatus: http.StatusBadRequest,
			body: `{"url":"https://example.com","status":"succeeded"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			memStore := NewMemoryStore()

			api := &API{store: memStore}

			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			rec := httptest.NewRecorder()

			api.ServeHTTP(rec, req)

			resp := rec.Result()

			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %v, want = %v", resp.StatusCode, tc.wantStatus)
			}

			j, err := memStore.Get(1)

			if !errors.Is(err, ErrJobNotFound) {
				t.Fatalf("rejected request created a job: %+v", j)
			}
		})
	}
}

func TestAPICreateJobStoreFailure(t *testing.T) {
	memStore := &failingStore{}

	api:=&API{store: memStore}

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"url":"https://example.com"}`))
	rec := httptest.NewRecorder()

	api.ServeHTTP(rec, req)

	resp := rec.Result()

	defer resp.Body.Close()

	byteBody, err := io.ReadAll(resp.Body)

	if (err != nil) {
		t.Fatalf("error reading response body: %v", err)
	}

	if resp.StatusCode != http.StatusInternalServerError || string(byteBody) != "failed to store job\n" {
		t.Fatalf("status = %v, body = %s; want status = %v, body = %s", resp.StatusCode, string(byteBody), http.StatusInternalServerError, "failed to store job\n")
	}
}

type fakeChecker struct {
	err error
}

func (c *fakeChecker) Check(url string) error {
	return c.err
}

func TestWorkerSucceeds (t *testing.T) {
	memStore := NewMemoryStore()
	err := memStore.Add(Job{
		ID: 1,
		URL:"https://example.com",
		Status: StatusQueued,
	})

	if err != nil {
		t.Fatalf("error adding job: %v", err)
	}

	fc := &fakeChecker{err: nil}

	ws := Worker{store: memStore, checker: fc}

	err = ws.RunOne(1)

	if err != nil {
		t.Fatalf("error running Worker.RunOne: %v", err)
	}

	j, err := ws.store.Get(1)

	if err != nil {
		t.Fatalf("error getting job: %v", err)
	}

	if j.Status != StatusSucceeded {
		t.Fatalf("status = %s, want = %s", j.Status, StatusSucceeded)
	}
}

func TestWorkerFails (t *testing.T) {
	memStore := NewMemoryStore()

	err := memStore.Add(Job{
		ID: 1,
		URL:"https://example.com",
		Status: StatusQueued,
	})

	if err != nil {
		t.Fatalf("error adding job: %v", err)
	}

	checkErr := errors.New("site unavailable")

	fc := &fakeChecker{err: checkErr}

	ws := Worker{store: memStore, checker: fc}

	err = ws.RunOne(1)

	if !errors.Is(err, checkErr) {
		t.Fatalf("error = %v, want = %v", err, checkErr)
	}

	j, err := ws.store.Get(1)

	if err != nil {
		t.Fatalf("error getting job: %v", err)
	}

	if j.Status != StatusFailed {
		t.Fatalf("status = %s, want = %s", j.Status, StatusFailed)
	}
}