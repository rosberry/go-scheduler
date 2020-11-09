package scheduler

import (
	"log"
	"time"

	"gorm.io/gorm"
)

const (
	SleepDuration = 30 * time.Second
)

type (
	TaskStatus uint

	//TaskManager base
	TaskManager struct {
		db *gorm.DB
		//locator locator.Locator
		funcs TaskFuncsMap
	}

	//Task ...
	Task struct {
		ID          uint `gorm:"primary_key"`
		Alias       string
		Status      TaskStatus
		Schedule    uint
		ScheduledAt time.Time
		CreatedAt   time.Time
		UpdatedAt   time.Time
	}

	//TaskFunc ...
	TaskFunc func() (status TaskStatus, when interface{})
	//TaskFuncsMap ...
	TaskFuncsMap map[string]TaskFunc
	// TaskFuncsCfg map[string]uint
)

const (
	TaskStatusWait TaskStatus = iota
	TaskStatusDeferred
	TaskStatusInProgress
	TaskStatusDone
)

// New создает экземпляр планировщика
func New(db *gorm.DB, funcs *TaskFuncsMap) *TaskManager {
	return &TaskManager{db, *funcs}
}

// Configure добавляет в планировщик (или обновляет существующие)
// обязательные задачи (карта, где ключом является alias задачи,
// а значением - интервал запуска в минутах).
func (tm *TaskManager) Configure(funcs map[string]uint) {
	for alias, schedule := range funcs {
		if _, ok := tm.funcs[alias]; ok {
			var task Task
			tm.db.FirstOrInit(&task, Task{Alias: alias})
			task.Schedule = schedule
			task.Status = TaskStatusWait
			if task.ID == 0 {
				task.ScheduledAt = time.Now()
			}
			tm.db.Save(&task)
		}
	}
}

// Run запускает бесконечный цикл планировщика
func (tm *TaskManager) Run() {
	for {
		var task Task
		tm.db.Where("status = ?", TaskStatusWait).Where("scheduled_at < ?", time.Now()).First(&task)
		if task.ID == 0 {
			time.Sleep(SleepDuration)
		} else {
			if fn, ok := tm.funcs[task.Alias]; ok {
				task.Status = TaskStatusInProgress
				tm.db.Save(&task)
				go tm.exec(&task, fn)
			} else {
				task.Status = TaskStatusDeferred
				tm.db.Save(&task)
			}
		}
	}
}

// Выполнение задачи с восстановлением из паники,
// смена статуса задачи и установка нового времени
// планирования задачи после ее выполнения.
func (tm *TaskManager) exec(task *Task, fn TaskFunc) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Scheduler][Recovery %s] panic recovered:\n%s\n\n", task.Alias, r)
			task.Status = TaskStatusWait
			if task.Schedule > 0 {
				task.ScheduledAt = task.ScheduledAt.Add(time.Minute * time.Duration(task.Schedule))
			}
			tm.db.Save(task)
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
	tm.db.Save(task)
}
