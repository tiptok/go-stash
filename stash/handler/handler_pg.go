package handler

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	jsoniter "github.com/json-iterator/go"
	"github.com/tal-tech/go-stash/stash/config"
	"github.com/tal-tech/go-stash/stash/filter"
	"github.com/tal-tech/go-zero/core/collection"
	"github.com/tal-tech/go-zero/core/threading"
)

type MessageHandlerPG struct {
	filters    []filter.FilterFunc
	db         *pg.DB
	conf       config.PostgresqlConf
	queue      *collection.Queue
	count      int32
	limitCount int32
}

func NewMessageHandlerPG(constant config.PostgresqlConf) *MessageHandlerPG {
	DB := pg.Connect(&pg.Options{
		User:     constant.User,
		Password: constant.Password,
		Database: constant.DBName,
		Addr:     fmt.Sprintf("%s:%s", constant.Host, constant.Port),
	})
	handler := &MessageHandlerPG{
		db:         DB,
		conf:       constant,
		queue:      collection.NewQueue(constant.MaxQueueSize),
		limitCount: int32(float64(constant.MaxQueueSize) * 0.9),
	}

	for i := 0; i < constant.ThreadSize; i++ {
		go handler.TimerConsume()
	}
	go handler.timerCreateLogTable()
	return handler
}

func (mh *MessageHandlerPG) AddFilters(filters ...filter.FilterFunc) {
	mh.filters = append(mh.filters, filters...)
}

func (mh *MessageHandlerPG) Consume(_, val string) error {
	var m map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(val), &m); err != nil {
		return err
	}

	for _, proc := range mh.filters {
		if m = proc(m); m == nil {
			return nil
		}
	}
	mh.queue.Put(m)
	atomic.AddInt32(&mh.count, 1)
	if mh.count > mh.limitCount {
		if _, ok := mh.queue.Take(); ok {
			atomic.AddInt32(&mh.count, -1)
		}
	}
	return nil
}

func (mh *MessageHandlerPG) TimerConsume() {
	t := time.Tick(time.Second * time.Duration(mh.conf.Interval))
	for range t {
		threading.RunSafe(
			func() {
				var logs []*Logs
				for i := 0; i < mh.conf.BatchSize; i++ {
					item, ok := mh.queue.Take()
					if !ok {
						break
					}
					atomic.AddInt32(&mh.count, -1)
					if m, ok := item.(map[string]interface{}); ok {
						logs = append(logs, &Logs{
							Log:     m,
							LogTime: time.Now(),
						})
					}
				}
				if len(logs) > 0 {
					_, err := mh.db.Model(&logs).Insert()
					fmt.Println(err)
				}
				fmt.Printf("logstash thread:%v queue:%v \n", threading.RoutineId(), mh.count)
			},
		)
	}
}

func (mh *MessageHandlerPG) timerCreateLogTable() {
	t := time.NewTimer(time.Hour * 6)
	for range t.C {
		threading.RunSafe(
			func() {
				mh.timerCreateLogTable()
			},
		)
	}
}

func (mh *MessageHandlerPG) TimerCreateLogTable() {
	var err error
	// creates database schema for Log models.
	err = mh.db.Model(&Logs{}).CreateTable(&orm.CreateTableOptions{
		IfNotExists: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	logStartTime := time.Now()
	logEndTime := logStartTime.AddDate(0, 3, 0)
	for logStartTime.Unix() <= logEndTime.Unix() {
		// Before insert, always try create partition
		err = createNewPartition(mh.db, logStartTime)
		if err != nil {
			log.Fatal(err)
		}
		// logData := &Logs{
		//     Log: map[string]interface{}{"msg":"test"},
		//     LogTime:   logStartTime,
		// }
		// _, err = mh.db.Model(logData).Insert()
		// if err != nil {
		//     log.Fatal(err)
		// }
		logStartTime = logStartTime.AddDate(0, 1, 0)
	}
}

func createNewPartition(db *pg.DB, currentTime time.Time) error {
	firstOfMonth := time.Date(currentTime.Year(), currentTime.Month(), 1, 0, 0, 0, 0, time.UTC)
	firstOfNextMonth := firstOfMonth.AddDate(0, 1, 0)

	year := firstOfMonth.Format("2006")
	month := firstOfMonth.Format("01")
	sql := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS logs_%s_%s PARTITION OF logs FOR VALUES FROM ('%s') TO ('%s');`,
		year, month,
		firstOfMonth.Format(time.RFC3339Nano),
		firstOfNextMonth.Format(time.RFC3339Nano),
	)

	_, err := db.Exec(sql)
	return err
}

type Logs struct {
	tableName struct{} `pg:"logs,partition_by:RANGE(log_time)"`
	Id        int      `pg:"id,pk"`
	Log       map[string]interface{}
	LogTime   time.Time `pg:"log_time,pk"`
}
