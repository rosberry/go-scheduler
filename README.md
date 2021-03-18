# Scheduler ![https://img.shields.io/badge/golang-1.15-blue](https://img.shields.io/badge/golang-1.15-blue)

This is a library to handle scheduled tasks.
The task list is stored in the database (GORM).

# Usage

1. Import the library.

```golang
import "github.com/rosberry/go-scheduler"
```

2. Use `gorm` ORM library to work with the database in the scheduler, create a model according to the example below and roll the migrations.

```golang
type Task struct {
    ID    uint `gorm:"primary_key"`
    Alias string

    Name       string
    Arguments  string
    Singletone bool

    Status      TaskStatus
    Schedule    uint
    ScheduledAt time.Time

    CreatedAt time.Time
    UpdatedAt time.Time
}
```
3. Create schedule functions and add them to the special map `scheduler.TaskFuncsMap`. Each function must return 2 parameters. The first is the status after the completion of the task. Possible options: `TaskStatusDone`, `TaskStatusWait`, `TaskStatusDeferred`. The second is how long it will take to call this task again. The passed types can be: `time.Duration`, `time.Time` or `nil`. If the time was not transmitted (nil was used), then we will use the static values stored in the database for this task. More on this later.

```golang

func PrintJobSingletone(args scheduler.FuncArgs) (status scheduler.TaskStatus, when interface{}) {
	log.Println("PrintJobSingletone:", time.Now())

	return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 1)
}

// in the running script
...
taskFuncsMap := scheduler.TaskFuncsMap{
	"upd_print": PrintJobSingletone,
}
```

4. Initialize the scheduler. gorm db in first function argument. The second argument is the `taskFuncMap` map declared earlier. The third argument is the time interval between checking tasks. By default it is 30 seconds and can be set up no less than this value.

```golang
sch := scheduler.New(db, &taskFuncsMap, sleepDuration)
```

5. You can add a static interval for the execution of tasks. If a time is specified for a task, then it will be singleton.

```golang
taskPlan := scheduler.TaskPlan{
	"upd_print": updPrintScheduleTimeout,
}
sch.Configure(taskPlan)
```

6. run scheduler in go routine

```golang
go sch.Run()
```

Full example:
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
		"upd_print":     PrintJobSingletone,
		"upd_printName": PrintWithArgs,
	}

	// Init TaskPlan for Singletone functions
	taskPlan := scheduler.TaskPlan{
		"upd_print": updPrintScheduleTimeout,
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
```

# Task types

- Singletone - unique task, can be only one task with alias in DB.
- Not singletone - dynamic task, that can be created while the program is running, supports arguments, there can be several tasks in the database with one alias.

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
