# Scheduler

This is a library to handle scheduled tasks.
The task list is stored in the database (GORM).


# Usage
1. Create table _Tasks_ in DB
```golang
type Task struct {
    ID        uint `gorm:"primary_key"`
    CreatedAt time.Time
    UpdatedAt time.Time

    Alias       string
    Status      uint
    Schedule    uint
    ScheduledAt time.Time
}
```


2. Usage
```golang
//Import package
import "github.com/rosberry/go-scheduler"

//Create schedule TaskFunc's
func PrintJob() (status scheduler.TaskStatus, when interface{}) {
	log.Println("PrintJob:", time.Now())

	return scheduler.TaskStatusWait, time.Now().Add(time.Minute * 10)
}

func PrintJobOneMinute() (status scheduler.TaskStatus, when interface{}) {
	log.Println("PrintJob (one minute):", time.Now())

	return scheduler.TaskStatusWait, nil
}

func main() {
    //Create FuncsMap and add our TaskFunc's
    var TaskFuncsMap = scheduler.TaskFuncsMap{
        "print": PrintJob,
        "printOneMinute": PrintJobOneMinute,
    }

    //Init scheduler
    sch := scheduler.New(db.DB, &TaskFuncsMap, time.Minute)

    //Configure task launch intervals
    tasks := make(scheduler.TaskPlan)
    tasks["printOneMinute"] = 1
    sch.Configure(tasks)

    //Add one-time (or autocontinue) task
    sch.Add("print", time.Now().Add(time.Minute*10))
    
    //Run scheduler
    go sch.Run()

    //Stub
    time.Sleep(time.Minute * 30)
    log.Println("exit")
}
```

# Roadmap
- [x] Scheduled tasks
- [x] Once tasks 
- [x] Concurency
- [x] Transactions
- [ ] Configure tasks
- [ ] ... 
