package scheduler

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	DefaultSleepDuration = 30 * time.Second
	MinimalSleepDuration = 1 * time.Second
)

type (
	TaskStatus uint

	// TaskManager base
	TaskManager struct {
		id            string
		db            *gorm.DB
		funcs         TaskFuncsMap
		sleepDuration time.Duration
	}

	// Task
	Task struct {
		ID    uint `gorm:"primary_key"`
		Alias string

		Name      string
		Arguments string
		Singleton bool

		Status      TaskStatus
		Schedule    uint
		ScheduledAt time.Time

		CreatedAt time.Time
		UpdatedAt time.Time
	}

	// Aggregate config for new scheduler
	Config struct {
		Sleep time.Duration
		Jobs  TaskSettings
	}

	// Aggregate settings for jobs
	TaskSettings map[string]struct {
		Func     TaskFunc
		Interval uint
	}

	// TaskFunc type func by task
	TaskFunc func(args FuncArgs) (status TaskStatus, when interface{})
	// TaskFuncsMap - list by TaskFunc's (key - task alias, value - TaskFunc)
	TaskFuncsMap map[string]TaskFunc

	// TaskPlan - list for initializing singleton tasks (key - task alias, value - start interval in minutes)
	TaskPlan map[string]uint
	FuncArgs map[string]interface{}
)

const (
	TaskStatusWait TaskStatus = iota
	TaskStatusDeferred
	TaskStatusInProgress
	TaskStatusDone
)

var ErrFuncNotFoundInTaskFuncsMap = errors.New("function not found in TaskFuncsMap")

// New TaskManager
func New(db *gorm.DB, funcs *TaskFuncsMap, sleepDuration time.Duration) *TaskManager {
	sleep := sleepDuration
	if sleep == 0 {
		sleep = DefaultSleepDuration
	} else if sleep < MinimalSleepDuration {
		sleep = MinimalSleepDuration
	}

	return &TaskManager{
		id:            uuid.New().String(),
		db:            db,
		funcs:         *funcs,
		sleepDuration: sleep,
	}
}

// New TaskManager with config
func NewWithConfig(db *gorm.DB, c Config) *TaskManager {
	taskFuncsMap := TaskFuncsMap{}
	taskPlan := TaskPlan{}

	for alias, item := range c.Jobs {
		taskFuncsMap[alias] = item.Func
		taskPlan[alias] = item.Interval
	}

	taskManager := New(db, &taskFuncsMap, c.Sleep)

	taskManager.Configure(taskPlan)

	return taskManager
}

// Configure add (or update if exist) tasks to TaskManager from TaskPlan
func (tm *TaskManager) Configure(funcs TaskPlan) {
	for alias, schedule := range funcs {
		if _, ok := tm.funcs[alias]; ok {
			var task Task

			tm.db.FirstOrInit(&task, Task{Alias: alias})

			if task.ID == 0 { // Add
				task.Schedule = schedule
				task.Status = TaskStatusWait
				task.ScheduledAt = time.Now()
				task.Singleton = true
				tm.db.Save(&task)
			} else if task.Schedule != schedule { // Update
				go func() {
					tm.db.Model(task).Update("schedule", schedule)
				}()
			}
		}
	}
}

// ClearTasks Removes tasks from DB if they are not in TaskManager
func (tm *TaskManager) ClearTasks() {
	dbTasks := make([]Task, 0)
	tm.db.Find(&dbTasks)

	for _, dbt := range dbTasks {
		if _, ok := tm.funcs[dbt.Alias]; !ok {
			go func() {
				tm.db.Delete(dbt)
			}()
		}
	}
}

// Run infinite TaskManager loop
func (tm *TaskManager) Run() {
	for {
		func() {
			tx := tm.db.Begin()

			defer func() {
				if r := recover(); r != nil {
					tx.Rollback()
				}
			}()

			var task Task
			err := tx.Raw(`
			UPDATE tasks SET updated_at = ?
			WHERE id = (
				SELECT id
				FROM tasks
				WHERE scheduled_at < now() AND status = ?
				ORDER BY scheduled_at LIMIT 1 FOR UPDATE SKIP LOCKED)
			RETURNING *;
			`,
				time.Now(),
				TaskStatusWait,
			).Scan(&task).Error
			if err != nil {
				log.Println(err)
			}

			if task.ID == 0 {
				tx.Rollback()
				time.Sleep(tm.sleepDuration)
			} else {
				if fn, ok := tm.funcs[task.Alias]; ok {
					go tm.exec(&task, fn, tx)
				} else {
					task.Status = TaskStatusDeferred
					tx.Save(&task)
					tx.Commit()
				}
			}
		}()
	}
}

// Executing a task with panic recovery,
// changing the task status and
// setting a new task scheduling time after its completion.
func (tm *TaskManager) exec(task *Task, fn TaskFunc, tx *gorm.DB) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Scheduler][Recovery %s] panic recovered:\n%s\n\n", task.Alias, r)
			task.Status = TaskStatusWait

			if task.Schedule > 0 {
				task.ScheduledAt = task.ScheduledAt.Add(time.Minute * time.Duration(task.Schedule))
			}

			tx.Save(task)
			tx.Commit()
		}
	}()

	funcArgs := task.ParseArgs()

	status, when := fn(funcArgs)
	switch status { // nolint:exhaustive TaskStatusInProgress = default
	case TaskStatusDone, TaskStatusWait, TaskStatusDeferred:
		task.Status = status
	default:
		task.Status = TaskStatusDeferred
	}

	switch v := when.(type) {
	case time.Duration:
		task.ScheduledAt = task.ScheduledAt.Add(v)
	case time.Time:
		task.ScheduledAt = v
	default:
		if task.Schedule > 0 {
			d := time.Minute * time.Duration(task.Schedule)
			task.ScheduledAt = task.ScheduledAt.Add(time.Now().Sub(task.ScheduledAt).Truncate(d) + d)
		} else {
			task.Status = TaskStatusDeferred
		}
	}
	tx.Save(task)
	tx.Commit()
}

// Add new no-Singleton task in DB
func (tm *TaskManager) Add(alias string, name string, args FuncArgs, runAt time.Time, intervalMinutes uint) error {
	if _, ok := tm.funcs[alias]; !ok {
		return ErrFuncNotFoundInTaskFuncsMap
	}

	task := Task{
		Alias:       alias,
		Name:        name,
		Status:      TaskStatusWait,
		ScheduledAt: runAt,
		Schedule:    intervalMinutes,
		Arguments:   args.String(),
	}

	return tm.db.Save(&task).Error
}

func (t *Task) ParseArgs() FuncArgs {
	if t.Arguments == "" {
		return nil
	}

	args := make(FuncArgs)

	err := json.Unmarshal([]byte(t.Arguments), &args)
	if err != nil {
		log.Print("ParseArgs() err:", err)
	}

	return args
}

func (args *FuncArgs) String() string {
	str, err := json.Marshal(args)
	if err != nil {
		log.Print("FuncArgs.String() err:", err)
	}

	return string(str)
}
