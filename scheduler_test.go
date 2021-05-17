package scheduler_test

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	. "github.com/smartystreets/goconvey/convey"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"go-scheduler"
)

const (
	dbConnectionError         = "db connection error"
	succeessChannelStatus     = "success"
	timeoutErrorChannelStatus = "error timeout operation"
)

var arrColNames = []string{"id", "alias", "name", "arguments", "singleton", "status", "schedule", "scheduled_at", "created_at", "updated_at"}

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
		Count uint
	}

	observerType struct {
		StatusChannel chan string
		TestName      string
		Count         uint
		Tasks         map[string]*observerTask
	}
)

var observer = observerType{
	StatusChannel: make(chan string, 10),
	Tasks:         make(map[string]*observerTask),
}

func TestStart(test *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.Stamp,
	})

	debugLevel, err := strconv.Atoi(os.Getenv("DEBUG"))

	zerolog.SetGlobalLevel(zerolog.Disabled)
	if err == nil && debugLevel == 1 {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	Convey("Given db connection settings", test, func() {
		connString := os.Getenv("DB_CONNECT_STRING")
		db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
		if err != nil {
			test.Errorf(err.Error())
			return
		}
		abortProcess := false

		Convey("\nWhen we want to check table with tasks and we want check this table correct structure\n", func() {
			err := db.Select(arrColNames).Find(&task{}).Error
			if err != nil {
				abortProcess = true
			}
			Convey("Database errors should be free", func() {
				So(err, ShouldBeNil)
			})
		})
		if abortProcess {
			return
		}

		Convey("\nWhen we want to make sure that the task is correct created in the database\n", func() {
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

			Convey("We check that we have not have an error in the process of adding a task", func() {
				So(err, ShouldBeNil)
			})
			if err != nil {
				return
			}

			respTask := &task{}
			err = db.Where(map[string]interface{}{
				"alias":     testObj.Alias,
				"name":      testObj.Name,
				"arguments": testObj.Arguments,
			}).First(respTask).Error
			Convey("We check that we have not have an error in the process of getting a task in database", func() {
				So(err, ShouldBeNil)
			})
			if err != nil {
				return
			}

			Convey("We expect that the test execution time of the task corresponds to the value of the task execution time from the database, accurate to the second", func() {
				testTime := testObj.ScheduledAt.Round(time.Second)
				respTime := respTask.ScheduledAt.Round(time.Second)
				So(testTime, ShouldEqual, respTime)
			})

			db.Where("alias = ?", testObj.Alias).Delete(&task{})
		})

		Convey("\nWhen we want to make sure that scheduler base run is correct\n", func() {
			testName := "simple run"
			const timeCoef uint = 3
			const rightCount uint = 2
			timeToRun := time.Duration(timeCoef) * time.Second

			taskName := "Test run with config"
			args := scheduler.FuncArgs{
				"name":       taskName,
				"duration":   timeToRun,
				"rightCount": rightCount,
			}

			testObj := task{
				Alias:     taskName,
				Arguments: args.String(),
			}

			taskManager, err := scheduler.NewWithConfig(scheduler.Config{
				Db:    db,
				Sleep: scheduler.MinimalSleepDuration,
				Jobs: scheduler.TaskSettings{
					testObj.Alias: {
						Interval: 1,
						Func:     channelJob,
						Args:     args,
					},
				},
			})
			Convey("We check that we have not have an error in the process of adding a task", func() {
				So(err, ShouldBeNil)
			})
			if err != nil {
				return
			}

			observer.TestName = testName
			observer.StatusChannel = make(chan string, 10)
			observer.Tasks = make(map[string]*observerTask)
			observer.Count = 0

			Convey("We check that the running jobs are running and we have no timeout error\n", func() {
				go taskManager.Run()
				go timeout(observer.TestName, timeToRun*time.Duration(rightCount+1), timeoutErrorChannelStatus)

				status := <-observer.StatusChannel
				switch status {
				case succeessChannelStatus:
					log.Info().
						Str("status", status).
						Uint("total count", observer.Count).
						Msgf("[%v: count %v]", testObj.Alias, observer.Tasks[testObj.Alias].Count)
				case timeoutErrorChannelStatus:
					log.Debug().Str("status", status).Uint("total count", observer.Count).Msg("")
				}
				So(status, ShouldEqual, succeessChannelStatus)
			})

			db.Where("alias = ?", testObj.Alias).Delete(&task{})
		})

		Convey("\nWhen we want to make sure that scheduler tasks status correct saving\n", func() {
			testName := "check status"
			var timeCoef uint = 1
			var rightCount uint = 2
			timeToRun := time.Duration(timeCoef) * time.Second

			taskName := "Test statuses"
			taskName2 := "Test2 statuses"
			taskManager, err := scheduler.NewWithConfig(scheduler.Config{
				Db:    db,
				Sleep: scheduler.MinimalSleepDuration,
				Jobs: scheduler.TaskSettings{
					taskName: {
						Interval: 1,
						Func:     channelJob,
						Args: scheduler.FuncArgs{
							"name":       taskName,
							"status":     scheduler.TaskStatusDeferred,
							"duration":   timeToRun,
							"rightCount": rightCount,
						},
					},
					taskName2: {
						Interval: 0,
						Func:     channelJob,
						Args: scheduler.FuncArgs{
							"name":        taskName2,
							"zeroTimeout": true,
							"duration":    timeToRun,
							"rightCount":  rightCount,
						},
					},
				},
			})

			Convey("We check that we have not have an error in the process of adding a task\n", func() {
				So(err, ShouldBeNil)
			})
			if err != nil {
				return
			}

			observer.TestName = testName
			observer.StatusChannel = make(chan string, 10)
			observer.Tasks = map[string]*observerTask{
				taskName:  {},
				taskName2: {},
			}
			observer.Count = 0

			Convey("We check that the task is in the deferred status and is not taken on subsequent iterations and\nWe check that if the task has a zero interval and the executing function returns the interval \"nil\", then the task goes into the \"deferred\" status and will not be processed later\n", func() {
				go taskManager.Run()
				go timeout(observer.TestName, timeToRun*time.Duration(rightCount+1), succeessChannelStatus)

				status := <-observer.StatusChannel
				log.Print("hopa")
				log.Info().
					Str("status", status).
					Uint("total count", observer.Count).
					Msgf("[%v: count %v, %v: count %v]", taskName, observer.Tasks[taskName].Count, taskName2, observer.Tasks[taskName2].Count)

				So(observer.Tasks[taskName].Count, ShouldEqual, 1)
				So(observer.Tasks[taskName2].Count, ShouldEqual, 1)
			})

			db.Where("alias = ?", taskName).Delete(&task{})
			db.Where("alias = ?", taskName2).Delete(&task{})
		})

		Convey("\nWhen we want to make sure that the scheduler does not take tasks taken by another scheduler\n", func() {
			testName := "db lock task"
			const timeCoef uint = 1
			const taskManagersCount uint = 10

			const wrongCount uint = taskManagersCount + 1
			const timeToRun = time.Duration(timeCoef) * time.Second
			const sleep = scheduler.MinimalSleepDuration

			taskName := "Test duplicate run with config"
			args := scheduler.FuncArgs{
				"name":     taskName,
				"duration": timeToRun,
				"tmCount":  taskManagersCount,
			}

			testObj := task{
				Alias:       taskName,
				ScheduledAt: time.Now().Add(timeToRun),
				Arguments:   args.String(),
			}

			taskManagers := [taskManagersCount]*scheduler.TaskManager{}
			taskManagers[0], err = scheduler.NewWithConfig(scheduler.Config{
				Db:    db,
				Sleep: sleep,
				Jobs: scheduler.TaskSettings{
					testObj.Alias: {
						Interval: 1,
						Func:     channelJob,
						Args:     args,
						RunAt:    testObj.ScheduledAt,
					},
				},
			})
			Convey("We check that we have not have an error in the process of adding a task", func() {
				So(err, ShouldBeNil)
			})
			if err != nil {
				return
			}
			for i := 1; i < int(taskManagersCount); i++ {
				taskManagers[i] = scheduler.New(
					db,
					&scheduler.TaskFuncsMap{
						testObj.Alias: channelJob,
					},
					sleep,
				)
			}
			log.Print("task managers count: ", len(taskManagers))

			observer.TestName = testName
			observer.StatusChannel = make(chan string, 10)
			observer.Tasks = make(map[string]*observerTask)
			observer.Count = 0

			Convey("We check that for a certain time the task was completed only once with another scheduler running in parallel\n", func() {
				for _, item := range taskManagers {
					go item.Run()
				}
				go timeout(observer.TestName, timeToRun+sleep, succeessChannelStatus)

				status := <-observer.StatusChannel
				log.Info().
					Uint("last count", observer.Count).
					Uint("wrong count value", wrongCount).
					Msgf("status: %s", status)

				So(wrongCount, ShouldBeGreaterThan, observer.Count)
			})

			db.Where("alias = ?", testObj.Alias).Delete(&task{})
		})
	})
}

func channelJob(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	name, hasName := args["name"]
	var nameStr string
	if hasName {
		nameStr = name.(string)
		if observer.Tasks[nameStr] != nil {
			observer.Tasks[nameStr].Count++
		} else {
			observer.Tasks[nameStr] = &observerTask{
				Count: 1,
			}
		}
		log.Info().Str("alias", nameStr).Msg("Channel job start")
	} else {
		log.Info().Msg("Channel job  start")
	}

	observer.Count++

	durationI, ok := args["duration"]
	var dur time.Duration
	if ok {
		durationF := durationI.(float64)
		dur = time.Duration(durationF)
	} else {
		dur = scheduler.DefaultSleepDuration
	}

	when = time.Now().Add(dur)
	tmCountI, checkTM := args["tmCount"]
	_, checkZeroTime := args["zeroTimeout"]
	if checkTM {
		tmCountF := tmCountI.(float64)
		if observer.Count <= uint(tmCountF) {
			log.Print("curr count: ", observer.Count)
			when = time.Now()
		}
	} else if checkZeroTime {
		when = nil
	}

	rightCountI, ok := args["rightCount"]
	if ok {
		rightCount := rightCountI.(float64)
		if observer.Count >= uint(rightCount) {
			observer.StatusChannel <- succeessChannelStatus
		}
	} else if !checkTM {
		observer.StatusChannel <- succeessChannelStatus
	}

	stI, ok := args["status"]
	if ok {
		log.Print("status: ", status)
		stF := stI.(float64)
		status = scheduler.TaskStatus(stF)
	}

	if hasName {
		log.Info().Str("alias", nameStr).Msg("Channel job finish")
	} else {
		log.Info().Msg("Channel job finish")
	}

	return status, when
}

func baseJob(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Info().Msg("Print base job")

	return status, time.Now().Add(1 * time.Second)
}

func timeout(testName string, dur time.Duration, returnType string) {
	time.Sleep(dur)
	if observer.TestName == testName {
		observer.StatusChannel <- returnType
	}
}

func createUniqueName() string {
	unixTime := time.Now().Unix()
	rand.Seed(unixTime)
	randTime := 11111111 + rand.Intn(88888888)
	randName := fmt.Sprintf(`%v_%v`, strconv.Itoa(int(unixTime)), strconv.Itoa(randTime))
	return randName
}
