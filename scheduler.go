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

	//Task ...
	Task struct {
		ID          uint `gorm:"primary_key"`
		Alias       string
		Status      TaskStatus
		Schedule    uint
		ScheduledAt time.Time
		CreatedAt   time.Time
		UpdatedAt   time.Time

		Worker string
		WorkAt *time.Time
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
			if task.ID == 0 { //Add
				task.Schedule = schedule
				task.Status = TaskStatusWait
				task.ScheduledAt = time.Now()
				tm.db.Save(&task)
			} else {
				if task.Schedule != schedule { //Update
					log.Printf("task.Schedule (%s) != schedule (%s)\n", task.Schedule, schedule)
					go func() {
						log.Println("[start] Update task:", alias) //FIXME: Debug, delete after all testing
						tm.db.Model(task).Update("schedule", schedule)
						log.Println("[end] Update task:", alias) //FIXME: Debug, delete after all testing
					}()
				}
			}
		}
	}
}

//ClearTasks удаляет из БД задачи, которых нет в TaskManager
func (tm *TaskManager) ClearTasks() {
	dbTasks := make([]Task, 0)
	tm.db.Find(&dbTasks)

	for _, dbt := range dbTasks {
		if _, ok := tm.funcs[dbt.Alias]; !ok {
			go func() {
				log.Println("[start] Delete task:", dbt.Alias) //FIXME: Debug, delete after all testing
				tm.db.Delete(dbt)
				log.Println("[end] Delete task:", dbt.Alias) //FIXME: Debug, delete after all testing
			}()
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
				time.Sleep(tm.sleepDuration) // (??) Sleep in transaction?
			} else {
				if fn, ok := tm.funcs[task.Alias]; ok {
					//task.Status = TaskStatusInProgress
					//tx.Save(&task)
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

// Add добавляет разовую (или самоуправляемую) задачу в планировщик. Если задача с таким alias уже есть в базе - обновляет scheduled_at
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
					log.Println("[start] Update task:", alias) //FIXME: Debug, delete after all testing
					tm.db.Model(task).Update("scheduled_at", runAt)
					log.Println("[end] Update task:", alias) //FIXME: Debug, delete after all testing
				}()
			}
		}
	}
}
