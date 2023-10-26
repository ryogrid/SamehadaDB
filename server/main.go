package main

import (
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"log"
	"net/http"
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

func postQuery(w rest.ResponseWriter, req *rest.Request) {
	input := QueryInput{}
	err := req.DecodeJsonPayload(&input)

	if err != nil {
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

func main() {
	api := rest.NewApi()

	// the Middleware stack
	api.Use(rest.DefaultDevStack...)
	api.Use(&rest.JsonpMiddleware{
		CallbackNameKey: "cb",
	})

	router, err := rest.MakeRouter(
		&rest.Route{"POST", "/Query", postQuery},
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
