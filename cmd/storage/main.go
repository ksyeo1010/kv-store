package main

import (
	"log"
	"flag"

	distkvs "example.org/cpsc416/a6"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config distkvs.StorageConfig
	err := distkvs.ReadJSONConfig("config/storage_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.StorageID, "id", config.StorageID, "Storage ID, e.g. storage1")
	flag.StringVar((*string)(&config.StorageAdd), "listen", string(config.StorageAdd), "Storage address, e.g. 127.0.0.1:5000")
	flag.StringVar(&config.DiskPath, "path", config.DiskPath, "Disk path, e.g /tmp/newdir")
	flag.Parse()

	log.Println(config)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.StorageID,
		Secret:         config.TracerSecret,
	})

	storage := distkvs.Storage{}
	err = storage.Start(config.StorageID, config.FrontEndAddr, string(config.StorageAdd), config.DiskPath, tracer)
	if err != nil {
		log.Fatal(err)
	}
}
