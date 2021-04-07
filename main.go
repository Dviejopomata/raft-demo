package main

import (
	"flag"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"github.com/gin-gonic/gin"
	"sync"
	"time"
)

type MyFsm struct {
	sync.Mutex
	logs    []byte
	LocalID string
}

func (m *MyFsm) Get() string {
	return string(m.logs)
}
func (m *MyFsm) Apply(log *raft.Log) interface{} {
	fmt.Printf("==== %s | %s | %s \n", m.LocalID, log.Data, time.Now().String())
	m.Lock()
	defer m.Unlock()
	m.logs = log.Data
	return len(m.logs)
}
func (m *MyFsm) Snapshot() (raft.FSMSnapshot, error) {
	m.Lock()
	defer m.Unlock()
	return &raft.MockSnapshot{}, nil
}

func (m *MyFsm) Restore(irc io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer irc.Close()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(irc, &hd)

	return dec.Decode("")
}

func main() {
	port := flag.Int("p", 7000, "port number")
	serverId := flag.String("i", "master", "server ID")
	isMaster := flag.Bool("m", false, "indicates the node is master")
	flag.Parse()

	address := fmt.Sprintf("127.0.0.1:%d", *port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		panic(err)
	}
	transport, err := raft.NewTCPTransport(address, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		panic(err)
	}
	hlog := hclog.New(&hclog.LoggerOptions{})
	fss, err := raft.NewFileSnapshotStoreWithLogger("", 1, hlog)
	if err != nil {
		panic(err)
	}
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(*serverId)
	mfsm := &MyFsm{LocalID: string(cfg.LocalID)}
	logstore := raft.NewInmemStore()
	r, err := raft.NewRaft(cfg, mfsm, logstore, logstore, fss, transport)
	if err != nil {
		panic(err)
	}
	var servers []raft.Server
	if *isMaster {
		servers = append(servers, raft.Server{
			ID:      cfg.LocalID,
			Address: transport.LocalAddr(),
		})
	}
	r.BootstrapCluster(raft.Configuration{Servers: servers})
	r1 := gin.Default()

	r1.GET("/", func(c *gin.Context) {
		stats := r.Stats()
		stats["leader"] = string(r.Leader())
		c.IndentedJSON(http.StatusOK, stats)
	})
	r1.GET("/put", func(c *gin.Context) {
		data, exists := c.GetQuery("data")
		if !exists {
			c.IndentedJSON(http.StatusUnprocessableEntity, map[string]interface{}{
				"error": "missing data parameter",
			})
			return
		}
		if r.State() != raft.Leader {
			c.IndentedJSON(http.StatusUnprocessableEntity, map[string]interface{}{
				"error": "not the leader",
			})
			return
		}

		applyFuture := r.Apply([]byte(data), 500*time.Millisecond)
		if err := applyFuture.Error(); err != nil {
			c.IndentedJSON(http.StatusUnprocessableEntity, map[string]interface{}{
				"error": fmt.Sprintf("error removing data in raft cluster: %s", err.Error()),
			})
			return
		}
		c.IndentedJSON(http.StatusOK, map[string]interface{}{
			"message": "success persisting data",
			"data":    data,
		})
	})

	r1.GET("/get", func(c *gin.Context) {
		data := mfsm.Get()
		c.IndentedJSON(http.StatusOK, map[string]interface{}{
			"message": "success get data",
			"data":    data,
		})
	})
	r1.GET("/state", func(c *gin.Context) {
		state := r.State().String()
		c.IndentedJSON(http.StatusOK, map[string]interface{}{
			"state": state,
		})
	})
	r1.GET("/join", func(c *gin.Context) {
		address, exists := c.GetQuery("address")
		if !exists {
			c.IndentedJSON(http.StatusUnprocessableEntity, map[string]interface{}{
				"error": "missing address parameter",
			})
			return
		}
		node, exists := c.GetQuery("node")
		if !exists {
			c.IndentedJSON(http.StatusUnprocessableEntity, map[string]interface{}{
				"error": "missing node parameter",
			})
			return
		}
		configFuture := r.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			c.IndentedJSON(http.StatusUnprocessableEntity, map[string]interface{}{
				"error": fmt.Sprintf("failed to get raft configuration: %s", err.Error()),
			})
			return
		}
		f := r.AddVoter(raft.ServerID(node), raft.ServerAddress(address), 0, 0)
		if f.Error() != nil {
			c.IndentedJSON(http.StatusUnprocessableEntity, map[string]interface{}{
				"error": fmt.Sprintf("error add voter: %s", f.Error().Error()),
			})
			return
		}
		stats := r.Stats()
		stats["leader"] = string(r.Leader())
		c.IndentedJSON(http.StatusOK, map[string]interface{}{
			"message": fmt.Sprintf("node %s at %s joined successfully", node, address),
			"data":    r.Stats(),
		})
	})

	r1.Run(fmt.Sprintf("0.0.0.0:%d", *port+10000))
	log.Print(r)
}
