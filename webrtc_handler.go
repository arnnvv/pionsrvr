package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
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

type DirectSignalPayloadClientToServer struct {
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
	ToPeerID  string                     `json:"toPeerID"`
	ClientID  string                     `json:"clientId,omitempty"`
}

type PayloadWithFrom struct {
	SDP        *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate  *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
	FromPeerID string                     `json:"fromPeerID"`
	ToPeerID   string                     `json:"toPeerID,omitempty"`
	ClientID   string                     `json:"clientId,omitempty"`
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
						if !p.isContextClosed() && !strings.Contains(err.Error(), "srtp: Subsession not found") && !strings.Contains(err.Error(), "io: read/write on closed pipe") {
							log.Printf("Peer %s: Error sending PLI for track %d: %v", p.id, track.SSRC(), err)
						}
					}
				case <-p.stopPLIGoroutine:
					log.Printf("Peer %s: PLI goroutine for track SSRC %d stopped via channel.", p.id, track.SSRC())
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
		log.Printf("Peer %s (HLS): ICE Connection State changed to %s", p.id, state.String())
		if state == webrtc.ICEConnectionStateFailed || state == webrtc.ICEConnectionStateClosed || state == webrtc.ICEConnectionStateDisconnected {
			log.Printf("Peer %s (HLS): ICE disconnected/closed/failed. Closing this specific peer connection context.", p.id)
			p.Close()
		}
	})

	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		pc.Close()
		return nil, fmt.Errorf("add video transceiver for HLS: %w", err)
	}
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		pc.Close()
		return nil, fmt.Errorf("add audio transceiver for HLS: %w", err)
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
			log.Printf("Peer %s: Error unmarshalling message: %v. Raw: %s", p.id, err, string(rawMsg))
			continue
		}

		p.mu.Lock()
		pcRef := p.pc
		p.mu.Unlock()

		if pcRef == nil && (msg.Type == "offer" || msg.Type == "candidate") {
			log.Printf("Peer %s: Received HLS '%s' but PeerConnection (for HLS) is nil (already closed). Ignoring.", p.id, msg.Type)
			continue
		}

		switch msg.Type {
		case "offer":
			var sdpPayload SDPPayload
			if err := json.Unmarshal(msg.Payload, &sdpPayload); err != nil {
				log.Printf("Peer %s: Error unmarshalling HLS offer payload: %v", p.id, err)
				continue
			}
			log.Printf("Peer %s: Received HLS offer", p.id)

			p.mu.Lock()
			pendingCandidatesRef := make([]*webrtc.ICECandidateInit, len(p.pendingCandidates))
			copy(pendingCandidatesRef, p.pendingCandidates)
			p.pendingCandidates = nil
			p.mu.Unlock()

			if err := pcRef.SetRemoteDescription(sdpPayload.SDP); err != nil {
				log.Printf("Peer %s: Error setting HLS remote description: %v", p.id, err)
				continue
			}

			answer, err := pcRef.CreateAnswer(nil)
			if err != nil {
				log.Printf("Peer %s: Error creating HLS answer: %v", p.id, err)
				continue
			}

			if err := pcRef.SetLocalDescription(answer); err != nil {
				log.Printf("Peer %s: Error setting HLS local description: %v", p.id, err)
				continue
			}

			for _, candInit := range pendingCandidatesRef {
				if err := pcRef.AddICECandidate(*candInit); err != nil {
					log.Printf("Peer %s: Error adding pending HLS ICE candidate: %v", p.id, err)
				} else {
					log.Printf("Peer %s: Added pending HLS ICE candidate: %s", p.id, candInit.Candidate)
				}
			}

			answerPayload, _ := json.Marshal(SDPPayload{SDP: answer})
			p.sendMessage(Message{Type: "answer", Payload: answerPayload})
			log.Printf("Peer %s: Sent HLS answer", p.id)

		case "candidate":
			var candidatePayload CandidatePayload
			if err := json.Unmarshal(msg.Payload, &candidatePayload); err != nil {
				log.Printf("Peer %s: Error unmarshalling HLS candidate payload: %v", p.id, err)
				continue
			}
			log.Printf("Peer %s: Received HLS ICE candidate: %s", p.id, candidatePayload.Candidate.Candidate)

			p.mu.Lock()
			currentPCRef := p.pc
			if currentPCRef != nil && currentPCRef.RemoteDescription() == nil {
				log.Printf("Peer %s: HLS Remote description not set. Queuing HLS candidate.", p.id)
				p.pendingCandidates = append(p.pendingCandidates, &candidatePayload.Candidate)
				p.mu.Unlock()
				continue
			}
			p.mu.Unlock()

			if currentPCRef == nil {
				log.Printf("Peer %s: Received HLS candidate but PeerConnection is nil after lock release. Ignoring.", p.id)
				continue
			}

			if err := currentPCRef.AddICECandidate(candidatePayload.Candidate); err != nil {
				log.Printf("Peer %s: Error adding HLS ICE candidate: %v", p.id, err)
			}

		case "signal-initiate-p2p", "direct-offer", "direct-answer", "direct-candidate":
			p.routeP2PMessage(msg)

		default:
			log.Printf("Peer %s: Unknown message type: %s", p.id, msg.Type)
		}
	}
}

func (p *PeerConnectionContext) routeP2PMessage(msg Message) {
	var clientPayload DirectSignalPayloadClientToServer
	if err := json.Unmarshal(msg.Payload, &clientPayload); err != nil {
		log.Printf("Peer %s: Error unmarshalling P2P client payload for type %s: %v. Raw: %s", p.id, msg.Type, err, string(msg.Payload))
		return
	}

	relayPayload := PayloadWithFrom{
		SDP:        clientPayload.SDP,
		Candidate:  clientPayload.Candidate,
		FromPeerID: p.id,
		ToPeerID:   clientPayload.ToPeerID,
		ClientID:   clientPayload.ClientID,
	}

	marshaledRelayPayload, err := json.Marshal(relayPayload)
	if err != nil {
		log.Printf("Peer %s: Error marshalling P2P relay payload for type %s: %v", p.id, msg.Type, err)
		return
	}
	messageToRelay := Message{Type: msg.Type, Payload: marshaledRelayPayload}

	streamerLock.RLock()
	defer streamerLock.RUnlock()

	if msg.Type == "signal-initiate-p2p" {
		announcerID := p.id
		log.Printf("Peer %s is announcing 'signal-initiate-p2p'. Broadcasting to others and informing announcer about existing peers.", announcerID)

		for _, existingPeerCtx := range streamerConnections {
			if existingPeerCtx.GetID() == announcerID {
				continue
			}

			log.Printf("Peer %s (announcer) signaling 'signal-initiate-p2p' to existing peer %s", announcerID, existingPeerCtx.GetID())
			existingPeerCtx.sendMessage(messageToRelay)

			payloadForAnnouncer := PayloadWithFrom{FromPeerID: existingPeerCtx.GetID()}
			marshaledPayloadForAnnouncer, mErr := json.Marshal(payloadForAnnouncer)
			if mErr != nil {
				log.Printf("Error marshalling 'signal-initiate-p2p' payload for announcer %s about existing peer %s: %v", announcerID, existingPeerCtx.GetID(), mErr)
				continue
			}
			msgForAnnouncer := Message{Type: "signal-initiate-p2p", Payload: marshaledPayloadForAnnouncer}

			log.Printf("Existing peer %s signaling 'signal-initiate-p2p' back to new peer %s (announcer)", existingPeerCtx.GetID(), announcerID)
			p.sendMessage(msgForAnnouncer)
		}
	} else {
		targetPeerID := clientPayload.ToPeerID
		if targetPeerID == "" {
			log.Printf("Peer %s: P2P message type %s is missing 'toPeerID' in payload.", p.id, msg.Type)
			return
		}

		foundTarget := false
		for _, targetCtx := range streamerConnections {
			if targetCtx.GetID() == targetPeerID {
				log.Printf("Peer %s routing P2P message '%s' (from %s) to target peer %s", p.id, msg.Type, relayPayload.FromPeerID, targetPeerID)
				targetCtx.sendMessage(messageToRelay)
				foundTarget = true
				break
			}
		}
		if !foundTarget {
			log.Printf("Peer %s: Target peer %s for P2P message type '%s' not found.", p.id, targetPeerID, msg.Type)
		}
	}
}

func (p *PeerConnectionContext) sendMessage(msg Message) {
	p.mu.Lock()
	wsRef := p.ws
	isCtxClosed := p.isClosed
	p.mu.Unlock()

	if isCtxClosed || wsRef == nil {
		log.Printf("Peer %s: Attempted to send message type '%s' but WebSocket or context is closed.", p.id, msg.Type)
		return
	}

	if err := wsRef.WriteJSON(msg); err != nil {
		log.Printf("Peer %s: Error writing JSON (type: %s) to WebSocket: %v", p.id, msg.Type, err)
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

	pcRef := p.pc
	wsRef := p.ws
	stopPLIGoCh := p.stopPLIGoroutine

	p.pc = nil
	p.ws = nil
	p.stopPLIGoroutine = nil
	p.mu.Unlock()

	if stopPLIGoCh != nil {
		select {
		case <-stopPLIGoCh:
		default:
			close(stopPLIGoCh)
		}
	}

	if p.hlsFeeder != nil {
		p.hlsFeeder.RemoveTracksByPeer(p.id)
	}

	if pcRef != nil {
		if err := pcRef.Close(); err != nil {
			log.Printf("Peer %s: Error closing HLS PeerConnection: %v", p.id, err)
		}
	}

	if wsRef != nil {
		wsRef.Close()
	}

	log.Printf("Peer %s: PeerConnectionContext closed.", p.id)
}
