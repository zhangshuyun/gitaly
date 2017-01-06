package router

import (
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"gitlab.com/gitlab-org/gitaly/handler"
)

func NewRouter() http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/", handler.Home)
	r.HandleFunc("/post-receive", handler.PostReceive)

	return handlers.LoggingHandler(os.Stdout, r)
}
