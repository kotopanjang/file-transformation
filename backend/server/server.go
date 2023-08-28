package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	startingMessage string = "HTTP Server starts to listen on %s"
	shutdownMessage string = "HTTP Server is gracefully shutdown."
)

// Server is a concrete struct of http server.
type Server struct {
	httpServer *http.Server
}

// NewServer is a constructor.
func NewServer(handler http.Handler, port string) *Server {
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		WriteTimeout: time.Second * 60,
		ReadTimeout:  time.Second * 30,
		IdleTimeout:  time.Second * 60,
		Handler:      handler,
	}

	return &Server{
		httpServer: httpServer,
	}
}

// Start will start the server.
// Do not call this in goroutine.
func (s *Server) Start() {
	go func() {
		log.Info().Msg(fmt.Sprintf(startingMessage, s.httpServer.Addr))
		if err := s.httpServer.ListenAndServe(); err != nil {
			log.Fatal().Msg(err.Error())
		}
	}()
}

// Close will block all the incomming request and subsequently shutdown the server.
func (s *Server) Close() {
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		log.Fatal().Msg(err.Error())
	}
	log.Info().Msg(shutdownMessage)
}
