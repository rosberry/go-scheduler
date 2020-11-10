package scheduler

import (
	"log"
	"time"

	"gorm.io/gorm"

	"github.com/google/uuid"
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

	//Task ...
	Task struct {
		ID          uint `gorm:"primary_key"`
		Alias       string
		Status      TaskStatus
		Schedule    uint
		ScheduledAt time.Time
		CreatedAt   time.Time
		UpdatedAt   time.Time
		Worker      string
	}

	//TaskFunc ...
	TaskFunc func() (status TaskStatus, when interface{})
	//TaskFuncsMap ...
	TaskFuncsMap map[string]TaskFunc

	//TaskPlan - список для первичной настройки задач (ключ - alias задачи, значение - интервал запуска в минутах)
	TaskPlan map[string]uint
)

const (
	TaskStatusWait TaskStatus = iota
	TaskStatusDeferred
	TaskStatusInProgress
	TaskStatusDone
)

// New создает экземпляр планировщика
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

// Configure добавляет в планировщик (или обновляет существующие)
// обязательные задачи (карта, где ключом является alias задачи,
// а значением - интервал запуска в минутах).
func (tm *TaskManager) Configure(funcs TaskPlan) {
	for alias, schedule := range funcs {
		if _, ok := tm.funcs[alias]; ok {
			var task Task
			tm.db.FirstOrInit(&task, Task{Alias: alias})
			if task.ID == 0 {
				task.Schedule = schedule
				task.Status = TaskStatusWait
				task.ScheduledAt = time.Now()
				tm.db.Save(&task)
			}

		}
	}
}

// Run запускает бесконечный цикл планировщика
func (tm *TaskManager) Run() {
	for {
		tm.db.Transaction(func(tx *gorm.DB) error {
			var task Task
			err := tx.Raw(`
			UPDATE tasks SET worker = ? 
			WHERE id = (SELECT id FROM tasks WHERE scheduled_at < now() and (worker is null or worker = '') ORDER BY scheduled_at LIMIT 1 FOR UPDATE SKIP LOCKED) 
			RETURNING *;
			`, tm.id).Scan(&task).Error
			if err != nil {
				log.Println(err)
			}

			if task.ID == 0 {
				time.Sleep(tm.sleepDuration)
			} else {
				if fn, ok := tm.funcs[task.Alias]; ok {
					task.Status = TaskStatusInProgress
					tx.Save(&task)
					tm.exec(&task, fn, tx)

				} else {
					task.Status = TaskStatusDeferred
					tx.Save(&task)
				}
			}
			return nil
		})
	}
}

// Выполнение задачи с восстановлением из паники,
// смена статуса задачи и установка нового времени
// планирования задачи после ее выполнения.
func (tm *TaskManager) exec(task *Task, fn TaskFunc, tx *gorm.DB) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Scheduler][Recovery %s] panic recovered:\n%s\n\n", task.Alias, r)
			task.Status = TaskStatusWait
			task.Worker = ""
			if task.Schedule > 0 {
				task.ScheduledAt = task.ScheduledAt.Add(time.Minute * time.Duration(task.Schedule))
			}
			tx.Save(task)
		}
	}()
	status, when := fn()
	switch status {
	case TaskStatusDone, TaskStatusWait, TaskStatusDeferred:
		task.Status = status
		task.Worker = ""
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
}

// Add добавляет разовую (или самоуправляемую) задачу в планировщик
func (tm *TaskManager) Add(alias string, runAt time.Time) {
	if _, ok := tm.funcs[alias]; ok {
		var task Task
		tm.db.FirstOrInit(&task, Task{Alias: alias})
		task.Status = TaskStatusWait

		task.ScheduledAt = runAt

		tm.db.Save(&task)
	}
}
