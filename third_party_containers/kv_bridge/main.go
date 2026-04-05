package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
)

// WriteRequest is the JSON envelope received on the MQTT topic.
// Value is json.RawMessage so it passes through any JSON type (object, string, number).
type WriteRequest struct {
	Bucket string          `json:"bucket"`
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value"`
	Op     string          `json:"op,omitempty"` // "delete" or empty (default: put)
}

// BucketPool caches open NATS KeyValue handles so we don't re-open per message.
type BucketPool struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	buckets map[string]nats.KeyValue
	mu      sync.Mutex
}

func NewBucketPool(nc *nats.Conn) (*BucketPool, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return &BucketPool{
		nc:      nc,
		js:      js,
		buckets: make(map[string]nats.KeyValue),
	}, nil
}

func (bp *BucketPool) Get(bucket string) (nats.KeyValue, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if kv, ok := bp.buckets[bucket]; ok {
		return kv, nil
	}

	// Try to bind to existing bucket first
	kv, err := bp.js.KeyValue(bucket)
	if err != nil {
		// Create if not exists
		kv, err = bp.js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  bucket,
			History: 1,
		})
		if err != nil {
			return nil, fmt.Errorf("create bucket %s: %w", bucket, err)
		}
	}

	bp.buckets[bucket] = kv
	return kv, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	mqttHost := env("MQTT_HOST", "mosquitto")
	mqttPort := env("MQTT_PORT", "1883")
	mqttTopic := env("MQTT_TOPIC", "kv_bridge/write")
	mqttClientID := env("MQTT_CLIENT_ID", "kv-bridge")
	natsURL := env("NATS_URL", "nats://nats:4222")
	logLevel := env("LOG_LEVEL", "info")

	debug := logLevel == "debug"

	log.Printf("kv-bridge starting")
	log.Printf("  MQTT: %s:%s topic=%s", mqttHost, mqttPort, mqttTopic)
	log.Printf("  NATS: %s", natsURL)

	// Connect to NATS
	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()
	log.Printf("NATS connected")

	pool, err := NewBucketPool(nc)
	if err != nil {
		log.Fatalf("bucket pool: %v", err)
	}

	// Stats
	var stats struct {
		received  uint64
		written   uint64
		deleted   uint64
		errors    uint64
		mu        sync.Mutex
	}

	// MQTT message handler
	handler := func(client mqtt.Client, msg mqtt.Message) {
		payload := msg.Payload()
		if len(payload) == 0 {
			return
		}

		var req WriteRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			stats.mu.Lock()
			stats.errors++
			stats.mu.Unlock()
			if debug {
				log.Printf("bad json: %v", err)
			}
			return
		}

		if req.Bucket == "" || req.Key == "" {
			stats.mu.Lock()
			stats.errors++
			stats.mu.Unlock()
			return
		}

		stats.mu.Lock()
		stats.received++
		stats.mu.Unlock()

		kv, err := pool.Get(req.Bucket)
		if err != nil {
			stats.mu.Lock()
			stats.errors++
			stats.mu.Unlock()
			log.Printf("bucket error %s: %v", req.Bucket, err)
			return
		}

		if req.Op == "delete" {
			if err := kv.Delete(req.Key); err != nil {
				stats.mu.Lock()
				stats.errors++
				stats.mu.Unlock()
				if debug {
					log.Printf("delete error %s/%s: %v", req.Bucket, req.Key, err)
				}
			} else {
				stats.mu.Lock()
				stats.deleted++
				stats.mu.Unlock()
				if debug {
					log.Printf("DEL %s/%s", req.Bucket, req.Key)
				}
			}
		} else {
			if _, err := kv.Put(req.Key, req.Value); err != nil {
				stats.mu.Lock()
				stats.errors++
				stats.mu.Unlock()
				if debug {
					log.Printf("put error %s/%s: %v", req.Bucket, req.Key, err)
				}
			} else {
				stats.mu.Lock()
				stats.written++
				stats.mu.Unlock()
				if debug {
					log.Printf("PUT %s/%s (%d bytes)", req.Bucket, req.Key, len(req.Value))
				}
			}
		}
	}

	// Connect to MQTT
	port, _ := strconv.Atoi(mqttPort)
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", mqttHost, port)).
		SetClientID(mqttClientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Printf("MQTT connected, subscribing to %s", mqttTopic)
			if token := c.Subscribe(mqttTopic, 1, handler); token.Wait() && token.Error() != nil {
				log.Printf("subscribe error: %v", token.Error())
			}
		})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("mqtt connect: %v", token.Error())
	}
	defer client.Disconnect(1000)

	// Stats reporter
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			stats.mu.Lock()
			r, w, d, e := stats.received, stats.written, stats.deleted, stats.errors
			stats.mu.Unlock()
			log.Printf("stats: received=%d written=%d deleted=%d errors=%d buckets=%d",
				r, w, d, e, len(pool.buckets))
		}
	}()

	// Wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Printf("shutdown on %v", s)

}
