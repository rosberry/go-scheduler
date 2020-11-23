package scheduler

import (
	"log"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	DefaultSleepDuration = 30 * time.Second
)

type (
	TaskStatus uint

	//TaskManager base
	TaskManager struct {
		id            string
		db            *gorm.DB
		funcs         TaskFuncsMap
		sleepDuration time.Duration
	}

	//Task
	Task struct {
		ID          uint `gorm:"primary_key"`
		Alias       string
		Status      TaskStatus
		Schedule    uint
		ScheduledAt time.Time
		CreatedAt   time.Time
		UpdatedAt   time.Time
	}

	//TaskFunc type func by task
	TaskFunc func() (status TaskStatus, when interface{})
	//TaskFuncsMap - list by TaskFunc's (key - task alias, value - TaskFunc)
	TaskFuncsMap map[string]TaskFunc

	//TaskPlan - list for initializing tasks (key - task alias, value - start interval in minutes)
	TaskPlan map[string]uint
)

const (
	TaskStatusWait TaskStatus = iota
	TaskStatusDeferred
	TaskStatusInProgress
	TaskStatusDone
)

// New TaskManager
func New(db *gorm.DB, funcs *TaskFuncsMap, sleepDuration time.Duration) *TaskManager {
	return &TaskManager{
		id:    uuid.New().String(),
		db:    db,
		funcs: *funcs,
		sleepDuration: func() time.Duration {
			if sleepDuration < DefaultSleepDuration {
				return DefaultSleepDuration
			}
			return sleepDuration
		}(),
	}
}

// Configure add (or update if exist) tasks to TaskManager from TaskPlan
func (tm *TaskManager) Configure(funcs TaskPlan) {
	for alias, schedule := range funcs {
		if _, ok := tm.funcs[alias]; ok {
			var task Task
			tm.db.FirstOrInit(&task, Task{Alias: alias})
			if task.ID == 0 { //Add
				task.Schedule = schedule
				task.Status = TaskStatusWait
				task.ScheduledAt = time.Now()
				tm.db.Save(&task)
			} else {
				if task.Schedule != schedule { //Update
					go func() {
						tm.db.Model(task).Update("schedule", schedule)
					}()
				}
			}
		}
	}
}

//ClearTasks Removes tasks from DB if they are not in TaskManager
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
			WHERE id = (SELECT id FROM tasks WHERE scheduled_at < now() ORDER BY scheduled_at LIMIT 1 FOR UPDATE SKIP LOCKED) 
			RETURNING *;
			`, time.Now()).Scan(&task).Error
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
	status, when := fn()
	switch status {
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

// Add a one-time (or self-managed) task to the scheduler.
// If a task with such a key is already in the database, it updates scheduled_at
func (tm *TaskManager) Add(alias string, runAt time.Time) {
	if _, ok := tm.funcs[alias]; ok {
		var task Task
		tm.db.FirstOrInit(&task, Task{Alias: alias})
		if task.ID == 0 { //Add
			task.Status = TaskStatusWait
			task.ScheduledAt = runAt
			tm.db.Save(&task)
		} else { //Update
			if task.ScheduledAt != runAt {
				go func() {
					tm.db.Model(task).Update("scheduled_at", runAt)
				}()
			}
		}
	}
}
