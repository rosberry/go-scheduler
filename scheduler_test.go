package scheduler_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"

	// . "github.com/smartystreets/goconvey/convey"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"go-scheduler"
)

type task struct {
	ID    uint `gorm:"primary_key"`
	Alias string

	Name      string
	Arguments string
	Singleton bool

	Status      scheduler.TaskStatus
	Schedule    uint
	ScheduledAt time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

var arrColNames = [...]string{"id", "alias", "name", "arguments", "singleton", "status", "schedule", "scheduled_at", "created_at", "updated_at"}

func TestStart(t *testing.T) {
	connString := os.Getenv("DB_CONNECT_STRING")
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	t.Run("Main=migration", func(t *testing.T) {
		migrator := db.Migrator()
		check := migrator.HasTable(task{})
		if check {
			if err := columnsCheck(migrator, task{}); err == nil {
				log.Print("Table already exist")
			} else {
				t.Errorf(err.Error())
			}
		} else {
			err := db.AutoMigrate(&task{})
			if err != nil {
				t.Errorf(err.Error())
			}
		}
	})

	// Create FuncsMap and add our TaskFunc's
	taskFuncsMap := scheduler.TaskFuncsMap{
		"test1": printJobSingleton,
		"test2": printWithArgs,
	}

	// Init TaskPlan for Singletone functions
	// taskPlan := scheduler.TaskPlan{
	// 	"upd_print": 5,
	// }

	taskManager := scheduler.New(db, &taskFuncsMap, scheduler.DefaultSleepDuration)

	t.Run("Main=add", func(t *testing.T) {
		err := taskManager.Add(
			"test1",
			"test1",
			scheduler.FuncArgs{"name": "Ivan"},
			time.Now().Add(time.Second*10),
			0,
		)
		if err != nil {
			t.Errorf(err.Error())
		}
	})

	t.Run("Main=run", func(t *testing.T) {
	})
}

func columnsCheck(migrator gorm.Migrator, t task) error {
	var errArr []string
	for _, col := range arrColNames {
		check := migrator.HasColumn(t, col)
		if !check {
			errArr = append(errArr, fmt.Sprintf("column %v not found.", col))
		}
	}
	if len(errArr) == 0 {
		return nil
	} else {
		errStr := strings.Join(errArr, "\n")
		return errors.New(errStr)
	}
}

// Create schedule TaskFunc's
// PrintJobSingletone ...
func printJobSingleton(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Print("PrintJobSingletone:", time.Now())

	return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 1)
}

// PrintWithArgs ...
func printWithArgs(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	if name, ok := args["name"]; ok {
		log.Print("PrintWithArgs:", time.Now(), name)
		return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 1)
	}

	log.Print("Not found name arg in func args")

	return scheduler.TaskStatusDeferred, time.Now().Add(time.Minute * 1)
}
