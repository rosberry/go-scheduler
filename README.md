# Scheduler ![https://img.shields.io/badge/golang-1.15-blue](https://img.shields.io/badge/golang-1.15-blue)

This is a library to handle scheduled tasks.
The task list is stored in the database (GORM).

## Base usage

1. Import the library.

```golang
import "github.com/rosberry/go-scheduler"
```
2. Use `gorm` ORM library to work with the database in the scheduler. You make use structure `scheduler.Task` for create migration.
```golang
tx := db.Begin()
tx.Migrator().AutoMigrate(&scheduler.Task{})
```

3. Initialize the scheduler with 3 arguments: `gorm db`, `taskFuncMap`, `sleepDuration`.<br> 
`gorm db`: instance `*gorm.DB` in gorm library.<br>
`taskFuncMap`: Before adding argument we should create schedule function(s) and add them to the special map `scheduler.TaskFuncsMap`. Each function must return 2 parameters. The first is the status after the completion of the task. Possible options: `TaskStatusDone`, `TaskStatusWait`, `TaskStatusDeferred`. The second is how long it will take to call this task again. The passed types can be: `time.Duration`, `time.Time` or `nil`. If the time was not transmitted (nil was used), then we will use the static values stored in the database for this task. More on this later.<br>
`sleepDuration`: time interval between checking tasks. By default it is 30 seconds and can be set up no less than this value.
```golang
printFunc := func(args scheduler.FuncArgs) (scheduler.TaskStatus, interface{}) {
	log.Print("Hello scheduler!")
	nextStart := time.Now().Add(time.Minute * 1)

	return scheduler.TaskStatusWait, nextStart
}

sch := scheduler.New(
	db, // gorm db
	&scheduler.TaskFuncsMap{
		"print": printFunc,
	}, 
	time.Minute * 5, // sleep duration
)

go sch.Run()
```
## Additional usage
You can also add a static interval for the execution of tasks. If a time is specified for a task, then it will be singleton.

```golang
taskPlan := scheduler.TaskPlan{
	"print": 5, // 5 minutes
}
sch.Configure(taskPlan)
// ...
go sch.Run()
```
You can simple inintialize scheduler with singleton and arguments:
```golang
sch, err := scheduler.NewWithConfig(scheduler.Config{
	Db:    db,
	Jobs: scheduler.TaskSettings{
		testObj.Alias: {
			Interval: 5, // 5 minutes
			Func:     printFunc, // job func
			Args: scheduler.FuncArgs{
				"name": "print",
			},
		},
	},
	Sleep: scheduler.MinimalSleepDuration,
})
```
You can create your task model for scheduler migration. To do this, make sure your structure meets the minimum requirements of the basic scheduler structure:
```golang
type Task struct {
    ID    uint `gorm:"primary_key"`
    Alias string

    Name       string
    Arguments  string
    Singleton bool

    Status      TaskStatus
    Schedule    uint
    ScheduledAt time.Time

    CreatedAt time.Time
    UpdatedAt time.Time
}
```

## Full example:
```golang
package main

import (
	"fmt"
	"log"
	"time"

	scheduler "github.com/rosberry/go-schedule"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	sleepDuration           = time.Minute
	updPrintScheduleTimeout = 5
)

func main() {
	// Connect to DB
	connString := "host=localhost port=5432 user=postgres dbname=clapper password=123 sslmode=disable"

	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Create FuncsMap and add our TaskFunc's
	taskFuncsMap := scheduler.TaskFuncsMap{
		"upd_print":     PrintJobSingleton,
		"upd_printName": PrintWithArgs,
	}

	// Init TaskPlan for Singleton functions
	taskPlan := scheduler.TaskPlan{
		"upd_print": updPrintScheduleTimeout,
	}

	// Init scheduler
	sch := scheduler.New(db, &taskFuncsMap, sleepDuration) // gorm db in first func argument

	// Add to DB and configure singleton tasks
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
// PrintJobSingleton ...
func PrintJobSingleton(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Println("PrintJobSingleton:", time.Now())

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
```

# Task types

- Singleton - unique task, can be only one task with alias in DB.
- Not singleton - dynamic task, that can be created while the program is running, supports arguments, there can be several tasks in the database with one alias.

# Roadmap

- [x] Scheduled tasks
- [x] Once tasks
- [x] Concurency
- [x] Transactions
- [ ] Configure tasks
- [ ] ...

## About

<img src="https://github.com/rosberry/Foundation/blob/master/Assets/full_logo.png?raw=true" height="100" />

This project is owned and maintained by [Rosberry](http://rosberry.com). We build mobile apps for users worldwide 🌏.

Check out our [open source projects](https://github.com/rosberry), read [our blog](https://medium.com/@Rosberry) or give us a high-five on 🐦 [@rosberryapps](http://twitter.com/RosberryApps).

## License

This project is available under the MIT license. See the LICENSE file for more info.
