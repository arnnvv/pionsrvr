package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v3"
)

const (
	maxHLSTracksDefault      = 2
	baseRTPPortDefault       = 5004
	rtpBufferSizeDefault     = 1500
	ffmpegRestartDelay       = 1 * time.Second
	ffmpegOutputCleanupDelay = 500 * time.Millisecond
	ffmpegStopTimeout        = 5 * time.Second
	rtpForwarderReadTimeout  = 5 * time.Second
	rtpForwarderInitialDelay = 1 * time.Second
)

type RTPForwarder struct {
	track    *webrtc.TrackRemote
	peerID   string
	conn     net.Conn
	rtpPort  int
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewRTPForwarder(track *webrtc.TrackRemote, peerID string, rtpPort int) (*RTPForwarder, error) {
	destAddr := fmt.Sprintf("127.0.0.1:%d", rtpPort)
	conn, err := net.Dial("udp", destAddr)
	if err != nil {
		return nil, fmt.Errorf("dial UDP for RTP forward to %s for peer %s track %s: %w", destAddr, peerID, track.ID(), err)
	}
	log.Printf("Peer %s: RTPForwarder created for track %s (%s, SSRC: %d) to %s", peerID, track.ID(), track.Codec().MimeType, track.SSRC(), destAddr)

	return &RTPForwarder{
		track:    track,
		peerID:   peerID,
		conn:     conn,
		rtpPort:  rtpPort,
		stopChan: make(chan struct{}),
	}, nil
}

func (f *RTPForwarder) Start() {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		defer func() {
			log.Printf("Peer %s: Closing UDP connection for track %s (SSRC: %d) to port %d", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort)
			if f.conn != nil {
				f.conn.Close()
			}
		}()

		log.Printf("Peer %s: RTPForwarder for track %s (SSRC: %d, port %d) DELAYING start by %s to allow FFmpeg to initialize.", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort, rtpForwarderInitialDelay)

		select {
		case <-time.After(rtpForwarderInitialDelay):
		case <-f.stopChan:
			log.Printf("Peer %s: RTPForwarder for track %s (SSRC: %d, port %d) stopped during initial delay.", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort)
			return
		}

		b := make([]byte, rtpBufferSizeDefault)
		packetsForwarded := 0
		firstPacketReceived := false
		log.Printf("Peer %s: Started RTP forwarder (after delay) for track %s (%s, SSRC: %d) to port %d. Waiting for packets...", f.peerID, f.track.ID(), f.track.Codec().MimeType, f.track.SSRC(), f.rtpPort)

		type readResult struct {
			n   int
			err error
		}
		readCh := make(chan readResult, 1)

		go func() {
			for {
				if f.track == nil {
					select {
					case readCh <- readResult{0, io.EOF}:
					case <-f.stopChan:
					}
					return
				}
				n, _, err := f.track.Read(b)
				select {
				case readCh <- readResult{n, err}:
				case <-f.stopChan:
					log.Printf("Peer %s: RTPForwarder track reader for SSRC %d detected stopChan, exiting reader.", f.peerID, f.track.SSRC())
					return
				}
				if err != nil {
					return
				}
			}
		}()

		for {
			select {
			case <-f.stopChan:
				log.Printf("Peer %s: Stopping RTP forwarder for track %s (SSRC: %d) to port %d. Forwarded %d packets.", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort, packetsForwarded)
				return

			case rr := <-readCh:
				if rr.err != nil {
					if rr.err == io.EOF {
						log.Printf("Peer %s: Track %s (SSRC: %d, port %d) ended (EOF). Forwarded %d packets.", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort, packetsForwarded)
					} else if !strings.Contains(rr.err.Error(), "use of closed network connection") &&
						!strings.Contains(rr.err.Error(), "RTPReceiver already closed") &&
						!strings.Contains(rr.err.Error(), "io: read/write on closed pipe") &&
						!strings.Contains(rr.err.Error(), "read udp") {
						log.Printf("Peer %s: Error reading from track %s (SSRC: %d, port %d): %v. Forwarded %d packets.", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort, rr.err, packetsForwarded)
					}
					return
				}

				if rr.n > 0 {
					if !firstPacketReceived {
						log.Printf("Peer %s: RTPForwarder for track %s (SSRC: %d, port %d) received FIRST packet (%d bytes). Forwarding...", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort, rr.n)
						firstPacketReceived = true
					}
					packetsForwarded++

					if f.conn == nil {
						log.Printf("Peer %s: UDP connection is nil for track %s (SSRC: %d), cannot write packet.", f.peerID, f.track.ID(), f.track.SSRC())
						continue
					}
					_, writeErr := f.conn.Write(b[:rr.n])

					if packetsForwarded <= 5 || packetsForwarded%100 == 0 {
						if writeErr == nil {
							log.Printf("Peer %s: RTPForwarder for track %s (SSRC: %d, port %d) successfully WROTE packet #%d (%d bytes) to UDP.", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort, packetsForwarded, rr.n)
						} else {
							log.Printf("Peer %s: RTPForwarder for track %s (SSRC: %d, port %d) FAILED to write packet #%d to UDP. Error: %v", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort, packetsForwarded, writeErr)
						}
					}

					if writeErr != nil {
						isConnRefused := false
						if opError, ok := writeErr.(*net.OpError); ok {
							if sErr, ok := opError.Err.(*os.SyscallError); ok {
								if strings.Contains(sErr.Err.Error(), "connection refused") {
									isConnRefused = true
								}
							}
						}
						if isConnRefused {
						} else if !strings.Contains(writeErr.Error(), "use of closed network connection") {
							log.Printf("Peer %s: Persistent error writing RTP packet to UDP port %d for track %s (SSRC: %d): %v", f.peerID, f.rtpPort, f.track.ID(), f.track.SSRC(), writeErr)
						}
					}
				}

			case <-time.After(rtpForwarderReadTimeout):
				if !firstPacketReceived && f.track != nil {
					log.Printf("Peer %s: RTPForwarder for track %s (SSRC: %d, port %d) still waiting for first packet...", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort)
				}
			}
		}
	}()
}

func (f *RTPForwarder) Stop() {
	log.Printf("Peer %s: Initiating stop for RTPForwarder for track %s (SSRC: %d) port %d", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort)
	close(f.stopChan)
	f.wg.Wait()
	log.Printf("Peer %s: RTPForwarder stopped for track %s (SSRC: %d) port %d", f.peerID, f.track.ID(), f.track.SSRC(), f.rtpPort)
}

type HLSFeederSlot struct {
	Port      int
	Codec     webrtc.RTPCodecCapability
	Forwarder *RTPForwarder
	PeerID    string
	IsVideo   bool
}

type HLSFeeder struct {
	mu            sync.Mutex
	slots         []*HLSFeederSlot
	ffmpegCmd     *exec.Cmd
	ffmpegSDPFile string
	hlsOutputDir  string
	ffmpegRunning bool
	tracksCount   int
	stopFFmpegCmd chan struct{}
	ffmpegWg      sync.WaitGroup
}

func NewHLSFeeder(sdpPath, hlsDir string) (*HLSFeeder, error) {
	feeder := &HLSFeeder{
		ffmpegSDPFile: sdpPath,
		hlsOutputDir:  hlsDir,
		slots:         make([]*HLSFeederSlot, maxHLSTracksDefault*2),
	}

	for i := range maxHLSTracksDefault {
		feeder.slots[i*2] = &HLSFeederSlot{
			Port:    baseRTPPortDefault + (i * 4),
			IsVideo: true,
		}
		feeder.slots[i*2+1] = &HLSFeederSlot{
			Port:    baseRTPPortDefault + (i * 4) + 2,
			IsVideo: false,
		}
	}
	return feeder, nil
}

func (f *HLSFeeder) generateSDP() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var sdpContent strings.Builder
	sdpContent.WriteString("v=0\n")
	sdpContent.WriteString(fmt.Sprintf("o=- %d %d IN IP4 127.0.0.1\n", time.Now().UnixNano(), time.Now().UnixNano()))
	sdpContent.WriteString("s=Pion HLS Stream\n")
	sdpContent.WriteString("c=IN IP4 127.0.0.1\n")
	sdpContent.WriteString("t=0 0\n")

	activeMediaDescriptions := 0
	videoTrackCount := 0
	audioTrackCount := 0

	for _, slot := range f.slots {
		if slot.Forwarder != nil && slot.Forwarder.track != nil {
			codec := slot.Codec
			pt := 0
			codecName := ""
			mediaType := ""
			clockRate := codec.ClockRate

			if slot.IsVideo {
				mediaType = "video"
				switch codec.MimeType {
				case webrtc.MimeTypeH264:
					pt = 102
					codecName = "H264"
				default:
					log.Printf("HLSFeeder: SDP generation skipping unsupported video codec %s for slot %d", codec.MimeType, slot.Port)
					continue
				}
				videoTrackCount++
			} else {
				mediaType = "audio"
				switch codec.MimeType {
				case webrtc.MimeTypeOpus:
					pt = 111
					codecName = "opus"
					if clockRate == 0 {
						clockRate = 48000
					}
				case webrtc.MimeTypePCMU:
					pt = 0
					codecName = "PCMU"
					if clockRate == 0 {
						clockRate = 8000
					}
				default:
					log.Printf("HLSFeeder: SDP generation skipping unsupported audio codec %s for slot %d", codec.MimeType, slot.Port)
					continue
				}
				audioTrackCount++
			}

			sdpContent.WriteString(fmt.Sprintf("m=%s %d RTP/AVP %d\n", mediaType, slot.Port, pt))
			sdpContent.WriteString(fmt.Sprintf("a=rtpmap:%d %s/%d", pt, codecName, clockRate))
			if !slot.IsVideo && codec.MimeType == webrtc.MimeTypeOpus && codec.Channels == 2 {
				sdpContent.WriteString("/2")
			}
			sdpContent.WriteString("\n")

			if slot.IsVideo && codec.MimeType == webrtc.MimeTypeH264 {
				sdpContent.WriteString(fmt.Sprintf("a=fmtp:%d packetization-mode=1;profile-level-id=42e01f;level-asymmetry-allowed=1\n", pt))
			} else if !slot.IsVideo && codec.MimeType == webrtc.MimeTypeOpus {
				sdpContent.WriteString(fmt.Sprintf("a=fmtp:%d minptime=10;useinbandfec=1\n", pt))
			}
			sdpContent.WriteString("a=recvonly\n")
			activeMediaDescriptions++
		}
	}

	if activeMediaDescriptions == 0 {
		log.Println("HLSFeeder: No active tracks for SDP. Removing SDP file if it exists.")
		if _, err := os.Stat(f.hlsOutputDir); !os.IsNotExist(err) {
			os.Remove(f.ffmpegSDPFile)
		}
		return nil
	}

	log.Printf("HLSFeeder: Generated SDP for FFmpeg (%d video, %d audio active media descriptions):\n%s", videoTrackCount, audioTrackCount, sdpContent.String())

	if err := os.MkdirAll(f.hlsOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create HLS output directory %s for SDP: %w", f.hlsOutputDir, err)
	}
	return os.WriteFile(f.ffmpegSDPFile, []byte(sdpContent.String()), 0644)
}

func (f *HLSFeeder) AddTrack(track *webrtc.TrackRemote, peerID string) error {
	f.mu.Lock()

	if f.tracksCount >= maxHLSTracksDefault*2 {
		log.Printf("HLSFeeder: Max HLS tracks (%d) reached. Ignoring new %s track from peer %s.", maxHLSTracksDefault*2, track.Kind(), peerID)
		f.mu.Unlock()
		return fmt.Errorf("max HLS tracks reached")
	}

	var targetSlot *HLSFeederSlot
	for _, slot := range f.slots {
		isKindMatch := (slot.IsVideo && track.Kind() == webrtc.RTPCodecTypeVideo) ||
			(!slot.IsVideo && track.Kind() == webrtc.RTPCodecTypeAudio)

		if slot.Forwarder == nil && isKindMatch {
			peerAlreadyHasKind := false
			for _, sCheck := range f.slots {
				if sCheck.Forwarder != nil && sCheck.PeerID == peerID && sCheck.IsVideo == slot.IsVideo {
					peerAlreadyHasKind = true
					break
				}
			}
			if !peerAlreadyHasKind {
				targetSlot = slot
				break
			}
		}
	}

	if targetSlot == nil {
		log.Printf("HLSFeeder: No available HLS slot for %s track from peer %s. Current total HLS tracks: %d.", track.Kind(), peerID, f.tracksCount)
		f.mu.Unlock()
		return fmt.Errorf("no available HLS slot for %s track from peer %s", track.Kind(), peerID)
	}

	forwarder, err := NewRTPForwarder(track, peerID, targetSlot.Port)
	if err != nil {
		f.mu.Unlock()
		return fmt.Errorf("create RTP forwarder for peer %s track %s: %w", peerID, track.ID(), err)
	}

	targetSlot.Forwarder = forwarder
	targetSlot.PeerID = peerID
	targetSlot.Codec = track.Codec().RTPCodecCapability
	f.tracksCount++
	log.Printf("HLSFeeder: Added %s track from peer %s (ID: %s, Codec: %s, SSRC: %d) to slot (port %d). Total HLS tracks: %d",
		track.Kind(), peerID, track.ID(), track.Codec().MimeType, track.SSRC(), targetSlot.Port, f.tracksCount)

	f.mu.Unlock()

	forwarder.Start()
	f.manageFFmpegState()
	return nil
}

func (f *HLSFeeder) RemoveTracksByPeer(peerID string) {
	var forwardersToStop []*RTPForwarder
	f.mu.Lock()

	log.Printf("HLSFeeder: Attempting to remove tracks for peer %s.", peerID)
	removedCount := 0
	for _, slot := range f.slots {
		if slot.Forwarder != nil && slot.Forwarder.peerID == peerID {
			forwardersToStop = append(forwardersToStop, slot.Forwarder)
			slot.Forwarder = nil
			slot.PeerID = ""
			f.tracksCount--
			removedCount++
		}
	}

	if removedCount == 0 {
		f.mu.Unlock()
		return
	}
	log.Printf("HLSFeeder: Marked %d HLS tracks for removal for peer %s. Total HLS tracks remaining: %d", removedCount, peerID, f.tracksCount)
	f.mu.Unlock()

	for _, fw := range forwardersToStop {
		fw.Stop()
	}
	log.Printf("HLSFeeder: Stopped %d forwarders for peer %s.", len(forwardersToStop), peerID)

	f.manageFFmpegState()
}

func (f *HLSFeeder) manageFFmpegState() {
	f.mu.Lock()

	shouldBeRunning := f.tracksCount > 0
	isCurrentlyRunning := f.ffmpegRunning

	f.mu.Unlock()

	if isCurrentlyRunning {
		if !shouldBeRunning {
			log.Println("HLSFeeder: manageFFmpegState - FFmpeg is running but should stop (no tracks).")
			f.signalStopFFmpeg()
			f.ffmpegWg.Wait()
			log.Println("HLSFeeder: manageFFmpegState - Old FFmpeg confirmed stopped (no tracks).")
		} else {
			log.Println("HLSFeeder: manageFFmpegState - FFmpeg is running and tracks configuration might have changed. Restarting.")
			f.signalStopFFmpeg()
			f.ffmpegWg.Wait()
			log.Println("HLSFeeder: manageFFmpegState - Old FFmpeg confirmed stopped for restart.")
			f.ensureFFmpegRunning()
			return
		}
	}

	if shouldBeRunning {
		f.ensureFFmpegRunning()
	} else {
		log.Println("HLSFeeder: manageFFmpegState - FFmpeg is not running and no tracks, nothing to do.")
	}
}

func (f *HLSFeeder) ensureFFmpegRunning() {
	f.mu.Lock()
	if f.ffmpegRunning {
		log.Printf("HLSFeeder: ensureFFmpegRunning - FFmpeg reported as already running. Tracks: %d", f.tracksCount)
		f.mu.Unlock()
		return
	}

	if f.tracksCount == 0 {
		log.Printf("HLSFeeder: ensureFFmpegRunning - No active tracks. FFmpeg will not be started.")
		f.mu.Unlock()
		return
	}
	f.mu.Unlock()

	if errSDP := f.generateSDP(); errSDP != nil {
		log.Printf("HLSFeeder: ensureFFmpegRunning - Error generating SDP: %v. FFmpeg will not be started.", errSDP)
		return
	}

	f.mu.Lock()
	sdpFi, err := os.Stat(f.ffmpegSDPFile)
	var sdpSize int64 = -1
	if sdpFi != nil {
		sdpSize = sdpFi.Size()
	}
	if os.IsNotExist(err) || err != nil || sdpSize == 0 {
		log.Printf("HLSFeeder: ensureFFmpegRunning - SDP file issue (path: %s, err: %v, size: %d). FFmpeg will not be started.", f.ffmpegSDPFile, err, sdpSize)
		f.mu.Unlock()
		return
	}

	log.Printf("HLSFeeder: ensureFFmpegRunning - Conditions met. Will attempt to start FFmpeg.")
	f.ffmpegRunning = true

	f.stopFFmpegCmd = make(chan struct{})
	f.ffmpegWg.Add(1)
	currentStopSignalForMonitor := f.stopFFmpegCmd
	f.mu.Unlock()

	errStart := f.startFFmpegInternal(currentStopSignalForMonitor)

	if errStart != nil {
		log.Printf("HLSFeeder: ensureFFmpegRunning - startFFmpegInternal FAILED: %v", errStart)
		f.mu.Lock()
		f.ffmpegRunning = false

		select {
		case <-currentStopSignalForMonitor:
		default:
			close(currentStopSignalForMonitor)
		}

		if f.stopFFmpegCmd == currentStopSignalForMonitor {
			f.stopFFmpegCmd = nil
		}
		f.ffmpegWg.Done()
		f.mu.Unlock()
		return
	}

	log.Println("HLSFeeder: ensureFFmpegRunning - FFmpeg process initiation successful via startFFmpegInternal.")
}

func (f *HLSFeeder) startFFmpegInternal(instanceStopChan <-chan struct{}) error {
	f.cleanupHLSOutputFiles()

	sdpBytes, err := os.ReadFile(f.ffmpegSDPFile)
	if err != nil {
		return fmt.Errorf("internal error: failed to read SDP file %s in startFFmpegInternal: %w", f.ffmpegSDPFile, err)
	}
	if len(sdpBytes) == 0 {
		log.Println("HLSFeeder: SDP file is empty. FFmpeg will not be started.")
		return nil
	}
	log.Printf("HLSFeeder: SDP file content for FFmpeg at start:\n%s", string(sdpBytes))

	ffmpegMapInputsForVideo := []string{}
	ffmpegMapInputsForAudio := []string{}
	currentVideoIdx := 0
	currentAudioIdx := 0

	scanner := bufio.NewScanner(strings.NewReader(string(sdpBytes)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "m=video") {
			ffmpegMapInputsForVideo = append(ffmpegMapInputsForVideo, fmt.Sprintf("0:v:%d", currentVideoIdx))
			currentVideoIdx++
		} else if strings.HasPrefix(line, "m=audio") {
			ffmpegMapInputsForAudio = append(ffmpegMapInputsForAudio, fmt.Sprintf("0:a:%d", currentAudioIdx))
			currentAudioIdx++
		}
	}

	if len(ffmpegMapInputsForVideo) == 0 && len(ffmpegMapInputsForAudio) == 0 {
		log.Println("HLSFeeder: No active video or audio m-lines in SDP for FFmpeg. Not starting FFmpeg.")
		return nil
	}
	log.Printf("HLSFeeder: FFmpeg resolved input mappings - Video: %v, Audio: %v", ffmpegMapInputsForVideo, ffmpegMapInputsForAudio)

	var complexFilter strings.Builder
	var outputVideoMap, outputAudioMap string

	if len(ffmpegMapInputsForVideo) > 0 {
		outputVideoMap = "[vout]"
		if len(ffmpegMapInputsForVideo) == 1 {
			complexFilter.WriteString(fmt.Sprintf("[%s]scale=640:360,setpts=PTS-STARTPTS%s", ffmpegMapInputsForVideo[0], outputVideoMap))
		} else if len(ffmpegMapInputsForVideo) >= 2 {
			complexFilter.WriteString(fmt.Sprintf(
				"[%s]scale=320:240,setpts=PTS-STARTPTS[v0];[%s]scale=320:240,setpts=PTS-STARTPTS[v1];[v0][v1]xstack=inputs=2:layout=0_0|w0_0%s",
				ffmpegMapInputsForVideo[0], ffmpegMapInputsForVideo[1], outputVideoMap))
			if len(ffmpegMapInputsForVideo) > 2 {
				log.Printf("HLSFeeder: More than 2 video tracks (%d), HLS will only show first two combined.", len(ffmpegMapInputsForVideo))
			}
		}
	}

	if len(ffmpegMapInputsForAudio) > 0 {
		outputAudioMap = "[aout]"
		if complexFilter.Len() > 0 && len(ffmpegMapInputsForVideo) > 0 {
			complexFilter.WriteString("; ")
		}
		var audioInputsForFilter strings.Builder
		for _, audioMap := range ffmpegMapInputsForAudio {
			audioInputsForFilter.WriteString(fmt.Sprintf("[%s]", audioMap))
		}
		complexFilter.WriteString(fmt.Sprintf("%samix=inputs=%d:normalize=0%s", audioInputsForFilter.String(), len(ffmpegMapInputsForAudio), outputAudioMap))
	}

	if err := os.MkdirAll(f.hlsOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create HLS output directory %s for FFmpeg: %w", f.hlsOutputDir, err)
	}

	ffmpegArgs := []string{
		"-protocol_whitelist", "file,udp,rtp",
		"-rtbufsize", "128M",
		"-nostdin",
		"-rw_timeout", "30000000",
		"-analyzeduration", "25000000",
		"-probesize", "20000000",
		"-max_delay", "15000000",
		"-fflags", "+nobuffer",
		"-fflags", "+ignidx",
		"-i", f.ffmpegSDPFile,
		"-y",
	}

	if complexFilter.Len() > 0 {
		log.Printf("HLSFeeder: Using FFmpeg filter_complex: %s", complexFilter.String())
		ffmpegArgs = append(ffmpegArgs, "-filter_complex", complexFilter.String())
	}

	if outputVideoMap != "" {
		ffmpegArgs = append(ffmpegArgs, "-map", outputVideoMap)
	} else if len(ffmpegMapInputsForVideo) > 0 && complexFilter.Len() == 0 {
		ffmpegArgs = append(ffmpegArgs, "-map", fmt.Sprintf("[%s]", ffmpegMapInputsForVideo[0]))
		log.Printf("HLSFeeder: Directly mapping video input %s as no complex video filter was generated.", ffmpegMapInputsForVideo[0])
	}
	if outputVideoMap != "" || (len(ffmpegMapInputsForVideo) > 0 && complexFilter.Len() == 0) {
		ffmpegArgs = append(ffmpegArgs,
			"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
			"-pix_fmt", "yuv420p", "-r", "25", "-g", "50",
			"-b:v", "800k", "-maxrate", "1000k", "-bufsize", "1500k",
		)
	}

	if outputAudioMap != "" {
		ffmpegArgs = append(ffmpegArgs, "-map", outputAudioMap)
	} else if len(ffmpegMapInputsForAudio) > 0 && complexFilter.Len() == 0 {
		ffmpegArgs = append(ffmpegArgs, "-map", fmt.Sprintf("[%s]", ffmpegMapInputsForAudio[0]))
		log.Printf("HLSFeeder: Directly mapping audio input %s as no complex audio filter was generated.", ffmpegMapInputsForAudio[0])
	}
	if outputAudioMap != "" || (len(ffmpegMapInputsForAudio) > 0 && complexFilter.Len() == 0) {
		ffmpegArgs = append(ffmpegArgs,
			"-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "2",
		)
	}

	hasVideoOutput := outputVideoMap != "" || (len(ffmpegMapInputsForVideo) > 0 && complexFilter.Len() == 0)
	hasAudioOutput := outputAudioMap != "" || (len(ffmpegMapInputsForAudio) > 0 && complexFilter.Len() == 0)

	if !hasVideoOutput && !hasAudioOutput {
		log.Println("HLSFeeder: No video or audio streams configured for output to HLS. FFmpeg not started.")
		return nil
	}

	ffmpegArgs = append(ffmpegArgs,
		"-f", "hls",
		"-hls_time", "2",
		"-hls_list_size", "5",
		"-hls_flags", "delete_segments+omit_endlist",
		"-hls_segment_filename", filepath.Join(f.hlsOutputDir, "segment_%05d.ts"),
		filepath.Join(f.hlsOutputDir, "stream.m3u8"),
	)

	log.Printf("HLSFeeder: Preparing to start FFmpeg with command: ffmpeg %s", strings.Join(ffmpegArgs, " "))

	cmd := exec.Command("ffmpeg", ffmpegArgs...)

	f.mu.Lock()
	f.ffmpegCmd = cmd
	f.mu.Unlock()

	stderrPipe, errPipeStderr := cmd.StderrPipe()
	if errPipeStderr != nil {
		log.Printf("HLSFeeder: ERROR creating StderrPipe for FFmpeg: %v", errPipeStderr)
	} else {
		go pipeToLog("[FFMPEG Stderr]", stderrPipe)
	}

	stdoutPipe, errPipeStdout := cmd.StdoutPipe()
	if errPipeStdout != nil {
		log.Printf("HLSFeeder: ERROR creating StdoutPipe for FFmpeg: %v", errPipeStdout)
	} else {
		go pipeToLog("[FFMPEG Stdout]", stdoutPipe)
	}

	log.Println("HLSFeeder: ATTEMPTING TO START FFMPEG PROCESS NOW...")
	if err := cmd.Start(); err != nil {
		log.Printf("HLSFeeder: FAILED TO START FFMPEG PROCESS: %v", err)
		f.mu.Lock()
		f.ffmpegCmd = nil
		f.mu.Unlock()
		return fmt.Errorf("failed to start FFmpeg process: %w", err)
	}
	log.Println("HLSFeeder: FFmpeg process cmd.Start() call SUCCEEDED.")

	go func(monitoredCmd *exec.Cmd, stopSignal <-chan struct{}) {
		defer func() {
			log.Println("HLSFeeder: FFmpeg monitoring goroutine exiting, calling f.ffmpegWg.Done()")
			f.ffmpegWg.Done()
			f.mu.Lock()
			if f.ffmpegCmd == monitoredCmd {
				f.ffmpegCmd = nil
			}
			f.ffmpegRunning = false
			f.mu.Unlock()
		}()

		processDone := make(chan error, 1)
		go func() {
			processDone <- monitoredCmd.Wait()
		}()

		select {
		case err := <-processDone:
			log.Printf("HLSFeeder: FFmpeg process exited. Error: %v", err)

			f.mu.Lock()
			tracksStillExist := f.tracksCount > 0
			isStopSignaled := false
			if stopSignal != nil {
				select {
				case <-stopSignal:
					isStopSignaled = true
				default:
				}
			}
			f.mu.Unlock()

			if tracksStillExist && !isStopSignaled {
				log.Println("HLSFeeder: FFmpeg exited unexpectedly, but tracks are still active. Attempting restart after delay.")
				time.Sleep(ffmpegRestartDelay)
				f.manageFFmpegState()
			} else if isStopSignaled {
				log.Println("HLSFeeder: FFmpeg exited due to a stop signal, not restarting automatically from monitor.")
				time.Sleep(ffmpegOutputCleanupDelay)
				f.cleanupHLSOutputFiles()
			} else {
				log.Println("HLSFeeder: FFmpeg exited and no active tracks or not signaled to stop. Cleaning up HLS output files.")
				time.Sleep(ffmpegOutputCleanupDelay)
				f.cleanupHLSOutputFiles()
			}

		case <-stopSignal:
			log.Printf("HLSFeeder: Received signal to stop FFmpeg via instanceStopChan.")
			if monitoredCmd.Process == nil {
				log.Println("HLSFeeder: FFmpeg process was nil when stop signal received for monitored command.")
				return
			}

			log.Printf("HLSFeeder: Signaling FFmpeg process (PID: %d) to interrupt.", monitoredCmd.Process.Pid)
			if err := monitoredCmd.Process.Signal(os.Interrupt); err != nil {
				log.Printf("HLSFeeder: Error sending SIGINT to FFmpeg (PID: %d), attempting kill: %v", monitoredCmd.Process.Pid, err)
				if monitoredCmd.Process != nil {
					monitoredCmd.Process.Kill()
				}
			}

			select {
			case err := <-processDone:
				log.Printf("HLSFeeder: FFmpeg (PID: %d) exited after interrupt signal. Error: %v", monitoredCmd.Process.Pid, err)
			case <-time.After(ffmpegStopTimeout):
				log.Printf("HLSFeeder: FFmpeg (PID: %d) did not stop within %s after interrupt, attempting kill again.", monitoredCmd.Process.Pid, ffmpegStopTimeout)
				if monitoredCmd.Process != nil {
					monitoredCmd.Process.Kill()
					<-processDone
				}
			}
			log.Println("HLSFeeder: FFmpeg process confirmed stopped via signal.")
			time.Sleep(ffmpegOutputCleanupDelay)
			f.cleanupHLSOutputFiles()
		}
	}(cmd, instanceStopChan)

	return nil
}

func (f *HLSFeeder) cleanupHLSOutputFiles() {
	filesTs, _ := filepath.Glob(filepath.Join(f.hlsOutputDir, "*.ts"))
	for _, file := range filesTs {
		os.Remove(file)
	}
	filesM3u8, _ := filepath.Glob(filepath.Join(f.hlsOutputDir, "*.m3u8"))
	for _, file := range filesM3u8 {
		os.Remove(file)
	}
	log.Printf("HLSFeeder: Cleaned HLS output files in: %s", f.hlsOutputDir)
}

func (f *HLSFeeder) signalStopFFmpeg() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.ffmpegRunning || f.stopFFmpegCmd == nil {
		log.Println("HLSFeeder: signalStopFFmpeg called, but FFmpeg not considered running or stopFFmpegCmd is nil.")
		return
	}

	log.Println("HLSFeeder: Signaling current FFmpeg process to stop via its dedicated stop channel...")
	select {
	case <-f.stopFFmpegCmd:
		log.Println("HLSFeeder: stopFFmpegCmd was already closed when signaling stop.")
	default:
		close(f.stopFFmpegCmd)
	}
	f.stopFFmpegCmd = nil
}

func (f *HLSFeeder) Stop() {
	f.mu.Lock()
	log.Println("HLSFeeder: Global Stop called. Stopping all forwarders...")
	var forwardersToStop []*RTPForwarder
	for _, slot := range f.slots {
		if slot.Forwarder != nil {
			forwardersToStop = append(forwardersToStop, slot.Forwarder)
			slot.Forwarder = nil
			slot.PeerID = ""
		}
	}
	f.tracksCount = 0
	ffmpegWasRunning := f.ffmpegRunning
	f.mu.Unlock()

	for _, fw := range forwardersToStop {
		fw.Stop()
	}
	log.Printf("HLSFeeder: Stopped %d forwarders during global stop.", len(forwardersToStop))

	if ffmpegWasRunning {
		log.Println("HLSFeeder: Global Stop - FFmpeg was running. Signaling it to stop.")
		f.signalStopFFmpeg()
		log.Println("HLSFeeder: Global Stop - Waiting for FFmpeg process to terminate...")
		f.ffmpegWg.Wait()
		log.Println("HLSFeeder: Global Stop - FFmpeg process confirmed terminated.")
	}

	f.cleanupHLSOutputFiles()

	f.mu.Lock()
	sdpPath := f.ffmpegSDPFile
	f.mu.Unlock()
	if _, err := os.Stat(sdpPath); !os.IsNotExist(err) {
		os.Remove(sdpPath)
	}
	log.Println("HLSFeeder: Fully stopped and cleaned up.")
}

func (f *HLSFeeder) Start() error {
	log.Println("HLSFeeder: Initialized. FFmpeg will start when tracks are added.")
	if err := os.MkdirAll(f.hlsOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create HLS output directory %s on start: %w", f.hlsOutputDir, err)
	}
	return nil
}

func pipeToLog(prefix string, pipe io.ReadCloser) {
	defer pipe.Close()
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		log.Printf("%s %s", prefix, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		if err != io.EOF && !strings.Contains(err.Error(), "file already closed") && !strings.Contains(err.Error(), "read/write on closed pipe") {
			log.Printf("%s Pipe error: %v", prefix, err)
		}
	}
}
