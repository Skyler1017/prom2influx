package main

import (
	"context"
	"github.com/google/logger"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb1-client"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type Trans struct {
	Database         string
	Start            time.Time
	End              time.Time
	Step             time.Duration
	promClient       v1.API
	influxClient     *client.Client
	numOfConnections int
	Retry            int
	MonitorLabel     string
	log              *logger.Logger
}

func NewTrans(database string, start, end time.Time, step time.Duration, p v1.API, i *client.Client,
	c, retry int, monitorLabel string, log *logger.Logger) *Trans {
	return &Trans{
		Database:         database,
		Start:            start,
		End:              end,
		Step:             step,
		promClient:       p,
		influxClient:     i,
		numOfConnections: c,
		Retry:            retry,
		MonitorLabel:     monitorLabel,
		log:              log,
	}
}

func (t *Trans) Run(ctx context.Context) error {
	Logger := t.log

	// query metric metricNames
	metricNames, warn, err := t.promClient.LabelValues(ctx, "__name__")
	_ = warn
	if err != nil {
		Logger.Info(err)
		return err
	}
	// parse time
	if t.End.IsZero() {
		t.End = time.Now()
	}
	if t.Step == 0 {
		t.Step = time.Minute
	}
	if t.Start.IsZero() {
		flags, err := t.promClient.Flags(ctx)
		if err != nil {
			return err
		}
		v, ok := flags["storage.tsdb.retention"]
		if !ok {
			panic("storage.tsdb.retention not found")
		}
		if v == "0s" {
			v = "15d"
		}
		if v[len(v)-1] == 'd' {
			a, err := strconv.Atoi(v[0 : len(v)-1])
			if err != nil {
				Logger.Error(err)
				return err
			}
			v = strconv.Itoa(a*24) + "h"
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			Logger.Error(err)
			return err
		}
		t.Start = time.Now().Add(-d)
	}
	if t.numOfConnections == 0 {
		t.numOfConnections = 1
	}

	c := make(chan struct{}, t.numOfConnections)
	wg := sync.WaitGroup{}
	wg.Add(len(metricNames))

	for _, metric := range metricNames {
		c <- struct{}{}
		// sync one metric
		go func(m string) {
			err := t.syncMetric(m)
			if err != nil {
				t.log.Error(m, err)
			}
			wg.Done()
			<-c
		}(string(metric))
	}
	wg.Wait()
	return nil
}

// 自动重试的 write 接口
func (t *Trans) sendBatchPoints(bps []client.BatchPoints) error {
	for _, bp := range bps {
		var err error
		for try := 0; try <= t.Retry; try++ {
			_, err = t.influxClient.Write(bp)
			if err == nil {
				break
			}
		}
		if err != nil {
			t.log.Error(err)
			return err
		}
	}
	t.log.Info("   ", len(bps), " batches migrated")
	return nil
}

func (t *Trans) syncMetric(metric string) error {
	Logger := t.log
	start := t.Start
	finish := t.End
	total := 0
	failCnt := 0
	step := t.End.Sub(t.Start)
	var bps []client.BatchPoints

	for start.Before(finish) {
		end := start.Add(step)
		ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
		v, warn, err := t.promClient.QueryRange(ctx, metric, v1.Range{
			Start: start,
			End:   end,
			Step:  t.Step,
		})
		if warn != nil {
			Logger.Info(warn)
		}
		if err != nil {
			failCnt += 1
			if failCnt < 15 {
				var after time.Duration
				after = time.Duration(step.Nanoseconds() / 2)
				//Logger.Warningf("【%4d】【%4d】->【%4d】    【%s】\n", int(step.Hours()), int(step.Hours()), int(after.Hours()), metric)
				step = after
				continue
			}
			Logger.Error(metric, err)
			return err
		}
		bps = append(bps, t.valueToInfluxdb(metric, v)...)
		Logger.Infof("【%4d】%4d points detected  【%s】", int(step.Hours()), len(bps), metric)

		if len(bps) > 6000 {
			err = t.sendBatchPoints(bps)
			if err != nil {
				Logger.Error(metric, err)
				return err
			}
			bps = nil
		}

		start = end
		total += len(bps)
	}

	if len(bps) > 0 {
		err := t.sendBatchPoints(bps)
		if err != nil {
			Logger.Error(metric, err)
			return err
		}
		bps = nil
	}
	Logger.Infof("【%s】 migrated %d records\n", metric, total)
	return nil
}

func (t *Trans) valueToInfluxdb(name string, v model.Value) (bps []client.BatchPoints) {
	var externalLabels = map[string]string{"monitor": t.MonitorLabel}
	switch v.(type) {
	case model.Matrix:
		v := v.(model.Matrix)
		for _, i := range v {
			bp := client.BatchPoints{
				Database:  t.Database,
				Tags:      metricToTag(i.Metric),
				Precision: "n",
			}
			for _, j := range i.Values {
				t := j.Timestamp.Time()
				bp.Points = append(bp.Points, client.Point{
					Tags:        externalLabels,
					Measurement: name,
					Time:        t,
					Fields:      map[string]interface{}{"value": float64(j.Value)},
				})
			}
			bps = append(bps, bp)
		}
	case *model.Scalar:
		v := v.(*model.Scalar)
		bps = append(bps, client.BatchPoints{
			Points: []client.Point{{
				Measurement: name,
				Tags:        externalLabels,
				Fields:      map[string]interface{}{"value": float64(v.Value)},
				Precision:   "n",
			}},
			Database: t.Database,
			Time:     v.Timestamp.Time().Add(-(time.Hour * 24 * 15)),
		})
	case model.Vector:
		v := v.(model.Vector)
		bp := client.BatchPoints{
			Database:  t.Database,
			Precision: "n",
		}
		for _, i := range v {
			tags := metricToTag(i.Metric)
			for k, v := range externalLabels {
				tags[k] = v
			}
			bp.Points = append(bp.Points, client.Point{
				Measurement: name,
				Tags:        tags,
				Time:        i.Timestamp.Time(),
				Fields:      map[string]interface{}{"value": i.Value},
			})
		}
		bps = append(bps, bp)
	case *model.String:
		v := v.(*model.String)
		bps = append(bps, client.BatchPoints{
			Points: []client.Point{{
				Measurement: name,
				Tags:        externalLabels,

				Fields:    map[string]interface{}{"value": v.Value},
				Precision: "ms",
			}},
			Database: t.Database,
			Time:     v.Timestamp.Time(),
		})
	default:
		panic("unknown type")
	}
	return
}

func metricToTag(metric model.Metric) map[string]string {
	return *(*map[string]string)(unsafe.Pointer(&metric))
}
