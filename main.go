package main

import (
	"context"
	"fmt"
	"github.com/google/logger"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb1-client"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

type config struct {
	verbose          bool
	influxdbURL      string
	prometheusURL    string
	monitorLabel     string
	influxdbDatabase string
	username         string
	password         string
	start            string
	end              string
	step             time.Duration
	c                int
	retry            int
}

var Logger = logger.Init("migrateLog", true, false, ioutil.Discard)

func parseConfig() *config {
	a := kingpin.New(filepath.Base(os.Args[0]), "Remote storage adapter")
	a.HelpFlag.Short('h')

	cfg := &config{}

	a.Flag("verbose", "print info level logs to stdout").
		Default("true").BoolVar(&cfg.verbose)
	a.Flag("influxdb-url", "The URL of the remote InfluxDB server to send samples to. None, if empty.").
		Default("").StringVar(&cfg.influxdbURL)
	a.Flag("username", "the username for influxDB.").
		Default("").StringVar(&cfg.username)
	a.Flag("password", "the password for influxDB.").
		Default("").StringVar(&cfg.password)
	a.Flag("prometheus-url", "The URL of the remote prometheus server to read samples to. None, if empty.").
		Default("").StringVar(&cfg.prometheusURL)
	a.Flag("monitor-label", "Prometheus Attach these labels to any time series or alerts when communicating with external systems. codelab-monitor, if empty.").
		Default("codelab-monitor").StringVar(&cfg.monitorLabel)
	a.Flag("influxdb.database", "The name of the database to use for storing samples in InfluxDB.").
		Default("prometheus").StringVar(&cfg.influxdbDatabase)
	a.Flag("start", "The time start.").
		Default("").StringVar(&cfg.start)
	a.Flag("end", "The time end").
		Default("").StringVar(&cfg.end)
	a.Flag("step", "The step").
		Default("1m").DurationVar(&cfg.step)
	a.Flag("c", "The connections").
		Default("1").IntVar(&cfg.c)
	a.Flag("retry", "The retry").
		Default("5").IntVar(&cfg.retry)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}
	return cfg
}

func main() {
	// parse config
	cfg := parseConfig()
	host, err := url.Parse(cfg.influxdbURL)
	if err != nil {
		log.Fatal(err)
	}
	// set log format
	logger.SetFlags(log.Ltime | log.Llongfile)
	if cfg.verbose {
		Logger.SetLevel(logger.Level(1))
	} else {
		Logger.SetLevel(logger.Level(3))
	}

	// parse time
	start, err := time.Parse(time.RFC3339, cfg.start)
	if err != nil {
		Logger.Info(err)
	}
	end, err := time.Parse(time.RFC3339, cfg.end)
	if err != nil {
		Logger.Info(err)
	}

	// get the influxDB client
	cli, err := client.NewClient(client.Config{
		URL:      *host,
		Username: cfg.username,
		Password: cfg.password,
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	// get the prometheus client
	c, _ := api.NewClient(api.Config{
		Address: cfg.prometheusURL,
	})
	newAPI := v1.NewAPI(c)

	// transfer historical data
	t := NewTrans(cfg.influxdbDatabase, start, end, cfg.step, newAPI,
		cli, cfg.c, cfg.retry, cfg.monitorLabel, Logger)
	Logger.Fatalln(t.Run(context.Background()))
}
