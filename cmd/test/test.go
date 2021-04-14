package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	distkvs "example.org/cpsc416/a5"
	"example.org/cpsc416/a5/kvslib"
)

// KVA is a key-value action
type KVA struct {
	action string
	key string
	value string
}

// Cli is a client id to []KVA map
type Cli struct {
	id string
	kva []KVA
}

// Req is a unique {clientID}:{action}:{key} to number map
type Req map[string]uint

func (r Req) addReq(id, action, key string) {
	k := id + ":" + action + "-" + key

	if _, ok := r[k]; !ok {
		r[k] = 1
	} else {
		r[k] += 1
	}
}

func (r Req)  buildReq(k string) string {
	s := strings.Split(k, "-")
	return s[0] + ":" + fmt.Sprintf("%v", r[k]) + ":" + s[1]
}

// cli1 performs some get and put requests
func cli1(id string) *Cli {
	return &Cli{
		id: id,
		kva: []KVA{
			{"get", "key1", ""},
			{"put", "key1", "val1"},
			{"get", "key1", ""},
			{"put", "key2", "val2"},
			{"get", "key2", ""},
		},
	}
}

// cli2 performs 100 KVA put requests
func cli2(id string) *Cli {
	c := &Cli{}
	c.id = id

	for i := 1; i <= 20; i++ {
		kva := KVA{"put", "key" + fmt.Sprintf("%v", i), "val" + fmt.Sprintf("%v",i)}
		c.kva = append(c.kva, kva)
	}

	return c
}

// cli3 performs 100 KVA get requests
func cli3(id string) *Cli {
	c := &Cli{}
	c.id = id

	for i := 1; i <= 20; i++ {
		kva := KVA{"get", "key" + fmt.Sprintf("%v", i), ""}
		c.kva = append(c.kva, kva)
	}

	return c
}

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	clis := []*Cli{cli1(config.ClientID), cli2(config.ClientID), cli3(config.ClientID)}

	length := 0
	for _, cli := range clis {
		length += len(cli.kva)

		go func(cli *Cli) {
			for _, kva := range cli.kva {
				// add delay
				time.Sleep(500 * time.Millisecond)
				if kva.action == "get" {
					if _, err := client.Get(cli.id, kva.key); err != nil {
						log.Println(err)
					}
				} else if kva.action == "put" {
					if _, err := client.Put(cli.id, kva.key, kva.value); err != nil {
						log.Println(err)
					}
				}
			}
		}(cli)
	} 

	for i := 0; i < length; i++ {
		<-client.NotifyChannel
	}

	// create req map
	req := make(Req, length)

	// start making exec command
	// app := "/bin/sh"
	arg0 := "./gradera6-smoketest"
	arg1 := "-t trace_output.log"
	arg2 := "-c " + config.ClientID
	arg3 := ""

	// build command
	for _, cli := range clis {
		for _, kva := range cli.kva {
			req.addReq(cli.id, kva.action, kva.key)
		}
	}

	for k := range req {
		arg3 += " -r " + req.buildReq(k)
	}

	// log for script usage
	fmt.Fprintf(os.Stdout, "%s %s %s %s", arg0, arg1, arg2, arg3)
}
