package main

import (
	"fmt"
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/server/signal_handle"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"net/http"
	"os"
)

type QueryInput struct {
	Query string
}

type Row struct {
	C []interface{}
}

type QueryOutput struct {
	Result []Row
	Error  string
}

var db = samehada.NewSamehadaDB("default", 5000) //5MB
var IsStopped = false

func postQuery(w rest.ResponseWriter, req *rest.Request) {
	if signal_handle.IsStopped {
		rest.Error(w, "Server is stopped", http.StatusGone)
		return
	}

	input := QueryInput{}
	err := req.DecodeJsonPayload(&input)

	if err != nil {
		fmt.Println(err)
		rest.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if input.Query == "" {
		rest.Error(w, "Query is required", 400)
		return
	}

	err2, results := db.ExecuteSQL(input.Query)
	if err2 != nil {
		rest.Error(w, err2.Error(), http.StatusBadRequest)
		return
	}

	rows := make([]Row, 0)
	for _, row := range results {
		rows = append(rows, Row{row})
	}

	w.WriteJson(&QueryOutput{
		rows, "SUCCESS",
	})
}

func postQueryMsgPack(w rest.ResponseWriter, req *rest.Request) {
	if signal_handle.IsStopped {
		http.Error(w.(http.ResponseWriter), "Server is stopped", http.StatusGone)
		return
	}

	input := QueryInput{}
	err := req.DecodeJsonPayload(&input)

	if err != nil {
		fmt.Println(err)
		http.Error(w.(http.ResponseWriter), err.Error(), http.StatusBadRequest)
		return
	}

	if input.Query == "" {
		http.Error(w.(http.ResponseWriter), "Query is required", 400)
		return
	}

	err2, results := db.ExecuteSQL(input.Query)
	if err2 != nil {
		http.Error(w.(http.ResponseWriter), err2.Error(), http.StatusBadRequest)
		return
	}

	rows := make([]Row, 0)
	for _, row := range results {
		rows = append(rows, Row{row})
	}

	ret := QueryOutput{rows, "SUCCESS"}
	b, err := msgpack.Marshal(&ret)
	if err != nil {
		panic(err)
	}

	//var decoded []Row
	//err = msgpack.Unmarshal(b, &decoded)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(decoded)

	w.Header().Set("Content-Type", "application/octent-stream")
	w.(http.ResponseWriter).Write(b)
}

func launchDBAndListen() {
	api := rest.NewApi()

	// the Middleware stack
	api.Use(rest.DefaultDevStack...)
	api.Use(&rest.JsonpMiddleware{
		CallbackNameKey: "cb",
	})
	api.Use(&rest.CorsMiddleware{
		RejectNonCorsRequests: false,
		OriginValidator: func(origin string, request *rest.Request) bool {
			return true
		},
		AllowedMethods:                []string{"POST"},
		AllowedHeaders:                []string{"Accept", "content-type"},
		AccessControlAllowCredentials: true,
		AccessControlMaxAge:           3600,
	})

	router, err := rest.MakeRouter(
		&rest.Route{"POST", "/Query", postQuery},
		&rest.Route{"POST", "/QueryMsgPack", postQueryMsgPack},
	)
	if err != nil {
		log.Fatal(err)
	}
	api.SetApp(router)

	log.Printf("Server started")
	log.Fatal(http.ListenAndServe(
		"0.0.0.0:19999",
		api.MakeHandler(),
	))
}

func main() {
	exitNotifyCh := make(chan bool, 1)

	// start signal handler thread
	go signal_handle.SignalHandlerTh(db, &exitNotifyCh)

	// start server
	go launchDBAndListen()

	// wait shutdown operation finished notification
	<-exitNotifyCh

	fmt.Println("Server is stopped gracefully")
	// exit process
	os.Exit(0)
}
