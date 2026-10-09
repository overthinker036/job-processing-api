package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
)

//Types and consts
type JobStatus string

const StatusQueued JobStatus = "queued"
const StatusRunning JobStatus = "running"
const StatusSucceeded JobStatus = "succeeded"
const StatusFailed JobStatus = "failed"

var ErrJobNotFound = errors.New("job not found")
var ErrJobExists = errors.New("job exists")
var ErrInvalidTransition = errors.New("invalid transition")


//Structs
type Job struct {
	ID uint64 `json:"id"`
	URL string `json:"url"`
	Status JobStatus `json:"status"`
}

type MemoryStore struct { //Is a JobReader, has Create & Get behaviors on the underlying map[uint64]Job
	mu sync.Mutex
	jobs map[uint64]Job
	lastID uint64
}

type API struct { //Has getJob, createJob & ServeHTTP behaviors
	store JobReader //Stores a JobReader with Create & Get behaviors, i.e. MemoryStore with a map[uint64]Job inside it
}

type CreateJobRequest struct {
	URL string `json:"url"`
}

//Interfaces
type JobReader interface {
	Get(id uint64) (Job, error)
	Create(address string) (Job, error)
}


//Struct methods:

//Job.Transition
func (j *Job) Transition(next JobStatus) error {
	var err error = nil

	switch j.Status {
	case StatusQueued: //Queued
		switch next {
			case StatusRunning: //Queued -> Running: allowed, err stays nil
				j.Status = next
			default: //Queued -> anything else: not allowed, err passed
				err = fmt.Errorf("%w: %q -> %q", ErrInvalidTransition, j.Status, next)
		}
	case StatusRunning: //Running
		switch next {
		case StatusSucceeded: // Running -> Succeeded: allowed, err stays nil
			j.Status = next
		case StatusFailed: //Running -> Failed: allowed, err stays nil
			j.Status = next
		default: //Running -> anythin else: not allowed, err passed
			err = fmt.Errorf("%w: %q -> %q", ErrInvalidTransition, j.Status, next)
		}
	default: //Succeeded, Failed -> anything else: not allowed, err passed
		err = fmt.Errorf("%w: %q -> %q", ErrInvalidTransition, j.Status, next)
	}

	return err
}	

//MemoryStore.Add
func (m *MemoryStore) Add(job Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.jobs[job.ID]

	if exists {
		return fmt.Errorf("job %v: %w", job.ID, ErrJobExists)
	} 

	m.jobs[job.ID] = job
	
	if job.ID > m.lastID {
		m.lastID = job.ID
	}
	
	return nil
}

//Memorystore.Get
func (m *MemoryStore) Get(id uint64) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, exists := m.jobs[id]

	if exists {
		return j, nil
	}

	return j, fmt.Errorf("job %d: %w", id, ErrJobNotFound)
}	


//MemoryStore.Transition
func (m *MemoryStore) Transition(id uint64, next JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, exists := m.jobs[id]

	if !exists {
		return fmt.Errorf("job %d: %w", id, ErrJobNotFound)
	}

	err := j.Transition(next)

	if (err != nil) {
		return fmt.Errorf("job %v: %w", id, err)
	}
	m.jobs[j.ID] = j
	return err
}


//API.getJob
func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	rawID := r.URL.Query().Get("id")

	//Parameter missing:
	if rawID == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseUint(rawID, 10, 64)

	//Parameter parsing error:
	if (err != nil) {
		http.Error(w, "invalid id", http.StatusBadRequest) //400, Invalid ID
		return
	}

	j, err := a.store.Get(id)

	//Getting job error:
	if (err != nil) {
		if errors.Is(err, ErrJobNotFound) {
			http.Error(w, "not found 1", http.StatusNotFound)
		} else {
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}

		return
	}

	data, err := json.Marshal(j)

	//Encoding error:
	if (err != nil) {
		http.Error(w, "failed to encode", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	_, err = w.Write(data)

	//Response writing error:
	if (err != nil) {
		fmt.Println("writing response failed: ", err)
		return
	}
}

//API.createJob
func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	var input CreateJobRequest

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(&input)

	if err != nil { //Decode error
		http.Error(w, "failed to decode",http.StatusBadRequest)
		return
	}

	if input.URL == "" { //Empty URL error
		http.Error(w, "empty url", http.StatusBadRequest)
		return
	}

	j, err := a.store.Create(input.URL)

	if err != nil {
		http.Error(w, "failed to store job", http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(j)

	if err != nil {
		http.Error(w, "failed to encode", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, err = w.Write(data)

	//Response writing error:
	if (err != nil) {
		fmt.Println("writing response failed: ", err)
		return
	}
}

//API.ServeHTTP
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//Unknown endpoint:
	if r.URL.Path != "/jobs" {
		http.Error(w, "unknown path", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		a.getJob(w, r)
		
	case http.MethodPost:
		a.createJob(w, r)

	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

//MemoryStore.Create
func (m *MemoryStore) Create(address string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastID++

	job := Job{
		ID:	m.lastID,
		URL: address,
		Status: StatusQueued,
	}

	m.jobs[m.lastID] = job

	return job, nil
}

//Normal functions:
func NewMemoryStore() *MemoryStore {
	jobs := make(map[uint64]Job)
	memStore := MemoryStore{jobs: jobs}

	return &memStore
}


//Main
func main() {
	memStore := NewMemoryStore()

	var err error

	err = memStore.Add(Job{ID: 1, URL: "https://go.dev", Status: StatusQueued})

	if (err != nil) {
		fmt.Println(err)
		return
	}

	err = http.ListenAndServe("127.0.0.1:8080", &API{store: memStore})

	if (err != nil) {
		fmt.Println(err)
		return
	}
}	