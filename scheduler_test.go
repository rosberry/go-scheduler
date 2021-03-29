package scheduler_test

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog/log"

	// . "github.com/smartystreets/goconvey/convey"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"go-scheduler"
)

const (
	succeessChannelStatus     = "success"
	timeoutErrorChannelStatus = "timeout error"
)

var (
	statusChannel  chan string
	counterChannel chan uint
)

type (
	task struct {
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

	observerTask struct {
		Name  string
		Count uint
	}

	observerType struct {
		StatusChannel chan string
		Count         uint
		Tasks         map[string]*observerTask
	}
)

var observer = observerType{
	StatusChannel: make(chan string, 10),
	Tasks:         make(map[string]*observerTask),
}

func TestStart(t *testing.T) {
	connString := os.Getenv("DB_CONNECT_STRING")
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	t.Run("Main=check_db_table", func(t *testing.T) {
		arrColNames := []string{"id", "alias", "name", "arguments", "singleton", "status", "schedule", "scheduled_at", "created_at", "updated_at"}
		err := db.Select(arrColNames).Find(&task{}).Error
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	})

	t.Run("Main=add", func(t *testing.T) {
		timeToRun := time.Second * 10
		args := scheduler.FuncArgs{
			"arg1": 1,
			"arg2": "test",
		}
		testObj := task{
			Alias:       "Test Alias " + createUniqueName(),
			Name:        "Test Name " + createUniqueName(),
			Arguments:   args.String(),
			ScheduledAt: time.Now().Add(timeToRun),
		}

		taskFuncsMap := scheduler.TaskFuncsMap{
			testObj.Alias: baseJob,
		}

		taskManager := scheduler.New(db, &taskFuncsMap, scheduler.DefaultSleepDuration)

		err := taskManager.Add(
			testObj.Alias,
			testObj.Name,
			args,
			testObj.ScheduledAt,
			0,
		)
		if err != nil {
			t.Errorf(err.Error())
			return
		}

		respTask := &task{}
		err = db.Where(map[string]interface{}{
			"alias":     testObj.Alias,
			"name":      testObj.Name,
			"arguments": testObj.Arguments,
		}).First(respTask).Error
		if err != nil {
			t.Errorf(err.Error())
		}

		if !testObj.ScheduledAt.Round(0).Equal(respTask.ScheduledAt) {
			t.Errorf("invalid scheduled at time")
		}

		db.Where("alias = ?", testObj.Alias).Delete(&task{})
	})

	t.Run("Main=run", func(t *testing.T) {
		var timeCoef uint = 3
		var c uint = 1
		timeToRun := time.Duration(timeCoef) * time.Second

		testName := "Test run Alias " + createUniqueName()
		args := scheduler.FuncArgs{
			// "name": testName,
			// "duration":   timeToRun,
			"rightCount": c,
		}

		testObj := task{
			Alias:       testName,
			Name:        "Test run Name " + createUniqueName(),
			Arguments:   args.String(),
			ScheduledAt: time.Now().Add(timeToRun),
		}

		taskFuncsMap := scheduler.TaskFuncsMap{
			testObj.Alias: channelJob,
		}

		taskManager := scheduler.New(db, &taskFuncsMap, scheduler.DefaultSleepDuration)

		_ = taskManager.Add(
			testObj.Alias,
			testObj.Name,
			args,
			testObj.ScheduledAt,
			timeCoef,
		)

		observer.StatusChannel = make(chan string, 10)

		go taskManager.Run()
		go timeout(timeToRun * 6)
		status := <-observer.StatusChannel
		switch status {
		case succeessChannelStatus:
			log.Printf("Status: %v, count: %v, [%v: count %v]", status, observer.Count, testObj.Alias, observer.Tasks[testObj.Alias].Count)
			// log.Printf("Status: %v", status)
		case timeoutErrorChannelStatus:
			t.Errorf("error timeout operation")
		}

		db.Where("alias = ?", testObj.Alias).Delete(&task{})
	})
}

func baseJob(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Print("Print base job:", time.Now())

	return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 1)
}

func channelJob(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Print("Channel job start: ", time.Now())

	// name, ok := args["name"]
	// if ok {
	// 	nameStr := name.(string)
	// 	if observer.Tasks[nameStr] != nil {
	// 		observer.Tasks[nameStr].Count++
	// 	} else {
	// 		observer.Tasks[nameStr] = &observerTask{
	// 			Name:  nameStr,
	// 			Count: 1,
	// 		}
	// 	}
	// }
	log.Debug().Interface("args", args).Send()
	rightCount, ok := args["rightCount"]
	log.Print("rk = ", rightCount)
	if ok {
		// if observer.Count >= rightCount.(uint) {
		// 	observer.StatusChannel <- succeessChannelStatus
		// }
		switch rightCount.(type) {
		case float64:
			rk := rightCount.(float64)
			log.Print(rk)
			fmt.Printf("type is %T\n", rk)
			log.Debug().Interface("args", args["rightCount"]).Send()
			log.Print(uint(rk))
			if observer.Count >= uint(rk) {
				observer.StatusChannel <- succeessChannelStatus
			}
		case uint:
			if observer.Count >= rightCount.(uint) {
				observer.StatusChannel <- succeessChannelStatus
			}
		default:
			log.Print("no")
		}
	} else {
		observer.StatusChannel <- succeessChannelStatus
	}
	observer.Count++

	durationI, ok := args["duration"]
	log.Print(durationI)
	var dur time.Duration
	if ok {
		dur = durationI.(time.Duration)
	} else {
		dur = time.Second * 10
	}
	log.Print("Channel job finish: ", time.Now())
	return scheduler.TaskStatusWait, time.Now().Add(dur)
	// return scheduler.TaskStatusWait, time.Now().Add(time.Second * 10)
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

func timeout(dur time.Duration) {
	time.Sleep(dur)
	observer.StatusChannel <- timeoutErrorChannelStatus
}

func createUniqueName() string {
	unixTime := time.Now().Unix()
	rand.Seed(unixTime)
	randTime := 11111111 + rand.Intn(88888888)
	randName := fmt.Sprintf(`%v_%v`, strconv.Itoa(int(unixTime)), strconv.Itoa(randTime))
	return randName
}
