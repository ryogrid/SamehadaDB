package main

import (
	"fmt"
	"github.com/ant0ine/go-json-rest/rest"
	"log"
	"net/http"
)

type QueryInput struct {
	Query string
}

type Rows struct {
	C []interface{}
}

type QueryOutput struct {
	Result []Rows
	ErrMsg string
}

func postQuery(w rest.ResponseWriter, req *rest.Request) {
	input := QueryInput{}
	err := req.DecodeJsonPayload(&input)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println(input.Query)

	if input.Query == "" {
		rest.Error(w, "Query is required", 400)
		return
	}

	log.Printf("%#v", input)

	w.WriteJson(&QueryOutput{
		[]Rows{Rows{[]interface{}{1, "hoge"}}, Rows{[]interface{}{1, "hoge"}}}, "",
	})
}

func main() {
	//db := samehada.NewSamehadaDB("hoge", 200)
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
		":9999",
		api.MakeHandler(),
	))
}
