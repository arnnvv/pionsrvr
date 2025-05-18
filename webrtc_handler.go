package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"
)

type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type SDPPayload struct {
	SDP webrtc.SessionDescription `json:"sdp"`
}

type CandidatePayload struct {
	Candidate webrtc.ICECandidateInit `json:"candidate"`
}

type PeerConnectionContext struct {
	ws                *websocket.Conn
	pc                *webrtc.PeerConnection
	hlsFeeder         *HLSFeeder
	id                string
	mu                sync.Mutex
	pendingCandidates []*webrtc.ICECandidateInit
	isClosed          bool
	stopPLIGoroutine  chan struct{}
}

func NewPeerConnectionContext(ws *websocket.Conn, feeder *HLSFeeder, clientID string) (*PeerConnectionContext, error) {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	m := &webrtc.MediaEngine{}
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000},
		PayloadType:        102,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("register H264 codec: %w", err)
	}
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"},
		PayloadType:        111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register Opus codec: %w", err)
	}
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: 8000},
		PayloadType:        0,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register PCMU codec: %w", err)
	}

	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		return nil, fmt.Errorf("register default interceptors: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(ir))
	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return nil, fmt.Errorf("create peer connection: %w", err)
	}

	p := &PeerConnectionContext{
		ws:               ws,
		pc:               pc,
		hlsFeeder:        feeder,
		id:               clientID,
		stopPLIGoroutine: make(chan struct{}),
	}

	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("Peer %s: Got %s track SSRC: %d, Codec: %s", p.id, track.Kind(), track.SSRC(), track.Codec().MimeType)
		if err := p.hlsFeeder.AddTrack(track, p.id); err != nil {
			log.Printf("Peer %s: Error adding track to HLS feeder: %v", p.id, err)
		}

		go func() {
			ticker := time.NewTicker(time.Second * 3)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					p.mu.Lock()
					pcRef := p.pc
					p.mu.Unlock()

					if pcRef == nil || pcRef.ICEConnectionState() == webrtc.ICEConnectionStateClosed ||
						pcRef.ICEConnectionState() == webrtc.ICEConnectionStateFailed ||
						pcRef.ICEConnectionState() == webrtc.ICEConnectionStateDisconnected {
						return
					}
					err := pcRef.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}})
					if err != nil {
						log.Printf("Peer %s: Error sending PLI for track %d: %v", p.id, track.SSRC(), err)
					}
				case <-p.stopPLIGoroutine:
					log.Printf("Peer %s: PLI goroutine for track %d stopped via channel.", p.id, track.SSRC())
					return
				}
			}
		}()
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		p.mu.Lock()
		wsRef := p.ws
		closedRef := p.isClosed
		p.mu.Unlock()

		if closedRef || wsRef == nil {
			return
		}

		candidateJSON, err := json.Marshal(c.ToJSON())
		if err != nil {
			log.Printf("Peer %s: Error marshalling ICE candidate: %v", p.id, err)
			return
		}
		msg := Message{Type: "candidate", Payload: candidateJSON}
		p.sendMessage(msg)
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("Peer %s: ICE Connection State changed to %s", p.id, state.String())
		if state == webrtc.ICEConnectionStateFailed || state == webrtc.ICEConnectionStateClosed || state == webrtc.ICEConnectionStateDisconnected {
			log.Printf("Peer %s: ICE disconnected/closed/failed. Closing peer connection context.", p.id)
			p.Close()
		}
	})

	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		pc.Close()
		return nil, fmt.Errorf("add video transceiver: %w", err)
	}
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		pc.Close()
		return nil, fmt.Errorf("add audio transceiver: %w", err)
	}

	return p, nil
}

func (p *PeerConnectionContext) GetID() string {
	return p.id
}

func (p *PeerConnectionContext) isContextClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isClosed
}

func (p *PeerConnectionContext) HandleMessages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Peer %s: Recovered in HandleMessages: %v", p.id, r)
		}
		p.Close()
	}()

	for {
		if p.isContextClosed() {
			return
		}

		_, rawMsg, err := p.ws.ReadMessage()
		if err != nil {
			if !p.isContextClosed() {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
					log.Printf("Peer %s: WebSocket read error: %v", p.id, err)
				} else {
					log.Printf("Peer %s: WebSocket connection closed by client or network error.", p.id)
				}
			}
			return
		}

		var msg Message
		if err := json.Unmarshal(rawMsg, &msg); err != nil {
			log.Printf("Peer %s: Error unmarshalling message: %v", p.id, err)
			continue
		}

		p.mu.Lock()
		pcRef := p.pc
		p.mu.Unlock()

		if pcRef == nil && (msg.Type == "offer" || msg.Type == "candidate") {
			log.Printf("Peer %s: Received %s but PeerConnection is nil (already closed). Ignoring.", p.id, msg.Type)
			continue
		}

		switch msg.Type {
		case "offer":
			var sdpPayload SDPPayload
			if err := json.Unmarshal(msg.Payload, &sdpPayload); err != nil {
				log.Printf("Peer %s: Error unmarshalling offer payload: %v", p.id, err)
				continue
			}
			log.Printf("Peer %s: Received offer", p.id)

			p.mu.Lock()
			pendingCandidatesRef := make([]*webrtc.ICECandidateInit, len(p.pendingCandidates))
			copy(pendingCandidatesRef, p.pendingCandidates)
			p.pendingCandidates = nil
			p.mu.Unlock()

			if err := pcRef.SetRemoteDescription(sdpPayload.SDP); err != nil {
				log.Printf("Peer %s: Error setting remote description: %v", p.id, err)
				continue
			}

			answer, err := pcRef.CreateAnswer(nil)
			if err != nil {
				log.Printf("Peer %s: Error creating answer: %v", p.id, err)
				continue
			}

			if err := pcRef.SetLocalDescription(answer); err != nil {
				log.Printf("Peer %s: Error setting local description: %v", p.id, err)
				continue
			}

			for _, candInit := range pendingCandidatesRef {
				if err := pcRef.AddICECandidate(*candInit); err != nil {
					log.Printf("Peer %s: Error adding pending ICE candidate: %v", p.id, err)
				} else {
					log.Printf("Peer %s: Added pending ICE candidate: %s", p.id, candInit.Candidate)
				}
			}

			answerPayload, _ := json.Marshal(SDPPayload{SDP: answer})
			p.sendMessage(Message{Type: "answer", Payload: answerPayload})
			log.Printf("Peer %s: Sent answer", p.id)

		case "candidate":
			var candidatePayload CandidatePayload
			if err := json.Unmarshal(msg.Payload, &candidatePayload); err != nil {
				log.Printf("Peer %s: Error unmarshalling candidate payload: %v", p.id, err)
				continue
			}
			log.Printf("Peer %s: Received ICE candidate: %s", p.id, candidatePayload.Candidate.Candidate)

			p.mu.Lock()
			currentPCRef := p.pc
			if currentPCRef != nil && currentPCRef.RemoteDescription() == nil {
				log.Printf("Peer %s: Remote description not set. Queuing candidate.", p.id)
				p.pendingCandidates = append(p.pendingCandidates, &candidatePayload.Candidate)
				p.mu.Unlock()
				continue
			}
			p.mu.Unlock()

			if currentPCRef == nil {
				log.Printf("Peer %s: Received candidate but PeerConnection is nil after lock release. Ignoring.", p.id)
				continue
			}

			if err := currentPCRef.AddICECandidate(candidatePayload.Candidate); err != nil {
				log.Printf("Peer %s: Error adding ICE candidate: %v", p.id, err)
			}

		case "signal-initiate-p2p", "direct-offer", "direct-answer", "direct-candidate":
			p.routeP2PMessage(msg)

		default:
			log.Printf("Peer %s: Unknown message type: %s", p.id, msg.Type)
		}
	}
}

func (p *PeerConnectionContext) routeP2PMessage(msg Message) {
	var genericPayload map[string]any
	if err := json.Unmarshal(msg.Payload, &genericPayload); err != nil {
		log.Printf("Peer %s: Error unmarshalling P2P payload for type %s: %v", p.id, msg.Type, err)
		return
	}

	genericPayload["fromPeerID"] = p.id

	var toPeerID string
	if msg.Type != "signal-initiate-p2p" {
		idVal, ok := genericPayload["toPeerID"].(string)
		if !ok || idVal == "" {
			log.Printf("Peer %s: P2P message type %s missing or empty toPeerID", p.id, msg.Type)
			return
		}
		toPeerID = idVal
	}

	modifiedPayload, err := json.Marshal(genericPayload)
	if err != nil {
		log.Printf("Peer %s: Error marshalling modified P2P payload: %v", p.id, err)
		return
	}
	relayMsg := Message{Type: msg.Type, Payload: modifiedPayload}

	streamerLock.RLock()
	defer streamerLock.RUnlock()

	foundTarget := false
	for _, peerCtx := range streamerConnections {
		if peerCtx.GetID() == p.id && msg.Type == "signal-initiate-p2p" {
			continue
		}

		if msg.Type == "signal-initiate-p2p" {
			peerCtx.sendMessage(relayMsg)
		} else {
			if peerCtx.GetID() == toPeerID {
				log.Printf("Peer %s routing %s to %s", p.id, msg.Type, toPeerID)
				peerCtx.sendMessage(relayMsg)
				foundTarget = true
				break
			}
		}
	}

	if msg.Type != "signal-initiate-p2p" && !foundTarget {
		log.Printf("Peer %s: Target peer %s for %s not found.", p.id, toPeerID, msg.Type)
	}
}

func (p *PeerConnectionContext) sendMessage(msg Message) {
	p.mu.Lock()
	wsRef := p.ws
	isCtxClosed := p.isClosed
	p.mu.Unlock()

	if isCtxClosed || wsRef == nil {
		return
	}

	if err := wsRef.WriteJSON(msg); err != nil {
		log.Printf("Peer %s: Error writing JSON to WebSocket: %v", p.id, err)
		go p.Close()
	}
}

func (p *PeerConnectionContext) Close() {
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return
	}
	p.isClosed = true
	log.Printf("Peer %s: Closing PeerConnectionContext...", p.id)

	if p.stopPLIGoroutine != nil {
		close(p.stopPLIGoroutine)
	}

	pcRef := p.pc
	wsRef := p.ws
	p.pc = nil
	p.ws = nil
	p.mu.Unlock()

	if p.hlsFeeder != nil {
		p.hlsFeeder.RemoveTracksByPeer(p.id)
	}

	if pcRef != nil {
		if err := pcRef.Close(); err != nil {
			log.Printf("Peer %s: Error closing PeerConnection: %v", p.id, err)
		}
	}

	if wsRef != nil {
		wsRef.Close()
	}

	log.Printf("Peer %s: PeerConnectionContext closed.", p.id)
}
