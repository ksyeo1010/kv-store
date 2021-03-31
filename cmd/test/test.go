package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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

func (r Req) getReq(id, action, key string) string {
	// build map key
	k := id + ":" + action + ":" + key

	if _, ok := r[k]; !ok {
		r[k] = 1
	} else {
		r[k] += 1
	}

	return id + ":" + action + ":" + fmt.Sprintf("%v", r[k]) + ":" + key
}

// cli1 performs some get and put requests
func cli1() *Cli {
	return &Cli{
		id: "clientID1",
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
func cli2() *Cli {
	c := &Cli{}
	c.id = "clientID2"

	for i := 1; i <= 5; i++ {
		kva := KVA{"put", "key" + fmt.Sprintf("%v", i), "val" + fmt.Sprintf("%v",i)}
		c.kva = append(c.kva, kva)
	}

	return c
}

// cli3 performs 100 KVA get requests
func cli3() *Cli {
	c := &Cli{}
	c.id = "clientID3"

	for i := 1; i <= 5; i++ {
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

	clis := []*Cli{cli1(), cli2(), cli3()}

	length := 0
	for _, cli := range clis {
		length += len(cli.kva)

		for _, kva := range cli.kva {
			// add delay
			time.Sleep(100 * time.Millisecond)
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
	} 

	for i := 0; i < length; i++ {
		<-client.NotifyChannel
	}

	// create req map
	req := make(Req, length)

	// start making exec command
	// app := "/bin/sh"
	arg0 := "./gradera5-smoketest"
	arg1 := "--trace-file trace_output.log"
	arg2 := "--client-id " + config.ClientID
	arg3 := ""

	// build command
	for _, cli := range clis {
		arg2 += " --client-id " + cli.id
		for _, kva := range cli.kva {
			arg3 += " --req " + req.getReq(cli.id, kva.action, kva.key)
		}
	}

	// log for script usage
	fmt.Fprintf(os.Stdout, "%s %s %s %s %s", arg0, arg1, arg2, arg3, "> grade.txt")
}
