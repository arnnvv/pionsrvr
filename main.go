package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	hlsOutputDirDefault   = "../hls_output"
	ffmpegSDPFileDefault  = "../hls_output/input.sdp"
	streamerWsPathDefault = "/ws/stream"
	hlsServePathDefault   = "/hls/"
	serverAddrDefault     = ":8080"
	serverShutdownTimeout = 5 * time.Second
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	streamerConnections = make(map[*websocket.Conn]*PeerConnectionContext)
	streamerLock        sync.RWMutex
	globalHLSFeeder     *HLSFeeder
)

func main() {
	if err := os.MkdirAll(hlsOutputDirDefault, 0755); err != nil {
		log.Fatalf("Failed to create HLS output directory %s: %v", hlsOutputDirDefault, err)
	}

	var err error
	globalHLSFeeder, err = NewHLSFeeder(ffmpegSDPFileDefault, hlsOutputDirDefault)
	if err != nil {
		log.Fatalf("Failed to initialize HLS Feeder: %v", err)
	}
	if err := globalHLSFeeder.Start(); err != nil {
		log.Printf("Warning: HLS Feeder initial start problem: %v", err)
	}

	http.HandleFunc(streamerWsPathDefault, handleStreamerConnections)
	fs := http.FileServer(http.Dir(hlsOutputDirDefault))
	http.Handle(hlsServePathDefault, http.StripPrefix(hlsServePathDefault, fs))

	log.Printf("Server starting on %s...", serverAddrDefault)

	httpServer := &http.Server{Addr: serverAddrDefault}
	go func() {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down server...")
	streamerLock.Lock()
	activeConnections := make([]*PeerConnectionContext, 0, len(streamerConnections))
	for _, pcCtx := range streamerConnections {
		activeConnections = append(activeConnections, pcCtx)
	}
	streamerConnections = make(map[*websocket.Conn]*PeerConnectionContext)
	streamerLock.Unlock()

	for _, pcCtx := range activeConnections {
		pcCtx.Close()
	}

	globalHLSFeeder.Stop()

	if err := httpServer.Shutdown(context.TODO()); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	log.Println("Server gracefully stopped.")
}

func handleStreamerConnections(w http.ResponseWriter, r *http.Request) {
	clientIDs, ok := r.URL.Query()["clientId"]
	if !ok || len(clientIDs[0]) < 1 {
		log.Printf("Connection rejected: clientId query parameter is missing")
		http.Error(w, "clientId query parameter is required", http.StatusBadRequest)
		return
	}
	clientID := clientIDs[0]

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection for client %s: %v", clientID, err)
		return
	}
	log.Printf("Streamer client connected: %s (Remote: %s)", clientID, ws.RemoteAddr())

	pcContext, err := NewPeerConnectionContext(ws, globalHLSFeeder, clientID)
	if err != nil {
		log.Printf("Failed to create PeerConnectionContext for client %s: %v", clientID, err)
		ws.Close()
		return
	}

	streamerLock.Lock()
	streamerConnections[ws] = pcContext
	streamerLock.Unlock()

	go pcContext.HandleMessages()

	originalCloseHandler := ws.CloseHandler()
	ws.SetCloseHandler(func(code int, text string) error {
		log.Printf("Streamer client %s disconnected: Code %d, Text: %s", pcContext.GetID(), code, text)

		pcContext.Close()

		streamerLock.Lock()
		delete(streamerConnections, ws)
		streamerLock.Unlock()

		if originalCloseHandler != nil {
			return originalCloseHandler(code, text)
		}
		return nil
	})
}
