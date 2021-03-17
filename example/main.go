package main

import (
	"fmt"
	"log"
	"time"

	scheduler "github.com/rosberry/go-schedule"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const sleepDuration = time.Minute

func main() {
	// Connect to DB
	connString := "host=localhost port=5432 user=postgres dbname=clapper password=123 sslmode=disable"
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Create FuncsMap and add our TaskFunc's
	taskFuncsMap := scheduler.TaskFuncsMap{
		"upd_print":     PrintJobSingletone,
		"upd_printName": PrintWithArgs,
	}

	// Init TaskPlan for Singletone functions
	taskPlan := scheduler.TaskPlan{
		"upd_print": 5,
	}

	// Init scheduler
	sch := scheduler.New(db, &taskFuncsMap, sleepDuration) // gorm db in first func argument

	// Add to DB and configure singletone tasks
	sch.Configure(taskPlan)

	// Run scheduler
	go sch.Run()

	for i := 0; i < 3; i++ {
		sch.Add(
			"upd_printName",
			fmt.Sprintf("upd_printName #%v", i),
			scheduler.FuncArgs{"name": fmt.Sprintf("Ivan %v", i)},
			time.Now().Add(time.Second*10),
			0,
		)
		time.Sleep(time.Second * 3)
	}

	// Stub
	time.Sleep(time.Hour)
	log.Println("exit")
}

// Create schedule TaskFunc's
// PrintJobSingletone ...
func PrintJobSingletone(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Println("PrintJobSingletone:", time.Now())

	return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 1)
}

// PrintWithArgs ...
func PrintWithArgs(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	if name, ok := args["name"]; ok {
		log.Println("PrintWithArgs:", time.Now(), name)
		return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 1)
	}

	log.Print("Not found name arg in func args")

	return scheduler.TaskStatusDeferred, time.Now().Add(time.Minute * 1)
}
