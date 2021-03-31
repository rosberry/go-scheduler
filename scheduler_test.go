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
	wrongCountChannelStatus   = "wrong count"
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
		Name  string
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

	Convey("Given db connection settings", test, func(convey C) {
		connString := os.Getenv("DB_CONNECT_STRING")
		db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
		if err != nil {
			test.Errorf(err.Error())
			return
		}
		abortProcess := false

		Convey("\nWhen we want to check table with tasks and we want check this table correct structure\n", func(convey C) {
			test.Run("Main=check_db_table", func(test *testing.T) {
				err := db.Select(arrColNames).Find(&task{}).Error
				if err != nil {
					abortProcess = true
				}
				convey.Convey("Database errors should be free", func() {
					convey.So(err, ShouldBeNil)
				})
			})
		})
		if abortProcess {
			return
		}

		Convey("\nWhen we want to make sure that the task is correct created in the database\n", func(convey C) {
			test.Run("Main=add", func(test *testing.T) {
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

				convey.Convey("We check that we have not have an error in the process of adding a task", func() {
					convey.So(err, ShouldBeNil)
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
				convey.Convey("We check that we have not have an error in the process of getting a task in database", func() {
					convey.So(err, ShouldBeNil)
				})
				if err != nil {
					return
				}

				convey.Convey("We expect that the test execution time of the task corresponds to the value of the task execution time from the database, accurate to the second", func() {
					testTime := testObj.ScheduledAt.Round(time.Second)
					respTime := respTask.ScheduledAt.Round(time.Second)
					convey.So(testTime, ShouldEqual, respTime)
				})

				db.Where("alias = ?", testObj.Alias).Delete(&task{})
			})
		})

		Convey("\nWhen we want to make sure that scheduler base run is correct\n", func(convey C) {
			test.Run("Main=run", func(t *testing.T) {
				var timeCoef uint = 3
				var rightCount uint = 2
				timeToRun := time.Duration(timeCoef) * time.Second

				testName := "Test run Alias " + createUniqueName()
				args := scheduler.FuncArgs{
					"name":       testName,
					"duration":   timeToRun,
					"rightCount": rightCount,
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

				taskManager := scheduler.New(db, &taskFuncsMap, time.Second*1)

				err = taskManager.Add(
					testObj.Alias,
					testObj.Name,
					args,
					testObj.ScheduledAt,
					timeCoef,
				)
				convey.Convey("We check that we have not have an error in the process of adding a task", func() {
					convey.So(err, ShouldBeNil)
				})
				if err != nil {
					return
				}

				observer.TestName = "Main=run"
				observer.StatusChannel = make(chan string, 10)
				observer.Tasks = make(map[string]*observerTask)
				observer.Count = 0

				convey.Convey("We check that the running jobs are running and we have no timeout error\n", func() {
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
					convey.So(status, ShouldEqual, succeessChannelStatus)
				})

				db.Where("alias = ?", testObj.Alias).Delete(&task{})
			})
		})
		Convey("\nWhen we want to make sure that the scheduler does not take tasks taken by another scheduler\n", func(convey C) {
			test.Run("Main=check duplicate", func(t *testing.T) {
				var timeCoef uint = 3
				var wrongCount uint = 2
				timeToRun := time.Duration(timeCoef) * time.Second

				testName := "Test run2 Alias " + createUniqueName()
				args := scheduler.FuncArgs{
					"name":       testName,
					"duration":   timeToRun,
					"wrongCount": wrongCount,
				}

				testObj := task{
					Alias:       testName,
					Name:        "Test2 run Name " + createUniqueName(),
					Arguments:   args.String(),
					ScheduledAt: time.Now().Add(timeToRun),
				}

				taskFuncsMap := scheduler.TaskFuncsMap{
					testObj.Alias: channelJob,
				}

				taskManager := scheduler.New(db, &taskFuncsMap, timeToRun)
				taskManager2 := scheduler.New(db, &taskFuncsMap, timeToRun)

				err = taskManager.Add(
					testObj.Alias,
					testObj.Name,
					args,
					testObj.ScheduledAt,
					timeCoef,
				)
				convey.Convey("We check that we have not have an error in the process of adding a task", func() {
					convey.So(err, ShouldBeNil)
				})
				if err != nil {
					return
				}

				observer.TestName = "Main=check duplicate"
				observer.StatusChannel = make(chan string, 10)
				observer.Tasks = make(map[string]*observerTask)
				observer.Count = 0

				convey.Convey("We check that for a certain time the task was completed only once with another scheduler running in parallel\n", func() {
					go taskManager.Run()
					go taskManager2.Run()
					go timeout(observer.TestName, timeToRun+time.Duration(time.Second), succeessChannelStatus)

					status := <-observer.StatusChannel
					log.Info().
						Uint("last count", observer.Count).
						Uint("wrong count value", wrongCount).
						Msgf("status: %s", status)

					convey.So(status, ShouldEqual, succeessChannelStatus)
				})

				db.Where("alias = ?", testObj.Alias).Delete(&task{})
			})
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
				Name:  nameStr,
				Count: 1,
			}
		}
		log.Info().Str("alias", nameStr).Msg("Channel job start")
	} else {
		log.Info().Msg("Channel job  start")
	}

	observer.Count++

	wrongCountI, checkWrong := args["wrongCount"]
	if checkWrong {
		wrongCount := wrongCountI.(float64)
		if observer.Count >= uint(wrongCount) {
			observer.StatusChannel <- wrongCountChannelStatus
		}
	}

	rightCountI, ok := args["rightCount"]
	if ok {
		rightCount := rightCountI.(float64)
		if observer.Count >= uint(rightCount) {
			observer.StatusChannel <- succeessChannelStatus
		}
	} else if !checkWrong {
		observer.StatusChannel <- succeessChannelStatus
	}

	durationI, ok := args["duration"]
	var dur time.Duration
	if ok {
		durationF := durationI.(float64)
		dur = time.Duration(durationF)
	} else {
		dur = scheduler.DefaultSleepDuration
	}

	if hasName {
		log.Info().Str("alias", nameStr).Msg("Channel job finish")
	} else {
		log.Info().Msg("Channel job finish")
	}
	return scheduler.TaskStatusWait, time.Now().Add(dur)
}

func baseJob(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Info().Msg("Print base job")

	return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 1)
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
