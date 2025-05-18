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
	ffmpegRestartDelay       = 3 * time.Second
	ffmpegOutputCleanupDelay = 1 * time.Second
	ffmpegStopTimeout        = 5 * time.Second
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
		return nil, fmt.Errorf("dial UDP for RTP forward to %s for peer %s: %w", destAddr, peerID, err)
	}
	log.Printf("Peer %s: RTPForwarder created for track %s (%s) to %s", peerID, track.ID(), track.Codec().MimeType, destAddr)

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
		defer f.conn.Close()
		b := make([]byte, rtpBufferSizeDefault)

		for {
			select {
			case <-f.stopChan:
				log.Printf("Peer %s: Stopping RTP forwarder for track %s to port %d", f.peerID, f.track.ID(), f.rtpPort)
				return
			default:
				n, _, err := f.track.Read(b)
				if err != nil {
					if err == io.EOF {
						log.Printf("Peer %s: Track %s ended (EOF)", f.peerID, f.track.ID())
					} else if !strings.Contains(err.Error(), "use of closed network connection") {
						log.Printf("Peer %s: Error reading from track %s: %v", f.peerID, f.track.ID(), err)
					}
					return
				}
				if _, err := f.conn.Write(b[:n]); err != nil {
					if !strings.Contains(err.Error(), "use of closed network connection") {
						log.Printf("Peer %s: Error writing RTP packet to UDP port %d: %v", f.peerID, f.rtpPort, err)
					}
				}
			}
		}
	}()
	log.Printf("Peer %s: Started RTP forwarder for track %s (%s) to port %d", f.peerID, f.track.ID(), f.track.Codec().MimeType, f.rtpPort)
}

func (f *RTPForwarder) Stop() {
	close(f.stopChan)
	f.wg.Wait()
	log.Printf("Peer %s: RTPForwarder stopped for track %s port %d", f.peerID, f.track.ID(), f.rtpPort)
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
			Codec:   webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000},
			IsVideo: true,
		}
		feeder.slots[i*2+1] = &HLSFeederSlot{
			Port:    baseRTPPortDefault + (i * 4) + 2,
			Codec:   webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
			IsVideo: false,
		}
	}
	return feeder, nil
}

func (f *HLSFeeder) generateSDP() error {
	var sdpContent strings.Builder
	sdpContent.WriteString("v=0\n")
	sdpContent.WriteString("o=- 0 0 IN IP4 127.0.0.1\n")
	sdpContent.WriteString("s=Pion HLS Stream\n")
	sdpContent.WriteString("c=IN IP4 127.0.0.1\n")
	sdpContent.WriteString("t=0 0\n")

	activeMediaLines := 0
	for _, slot := range f.slots {
		if slot.Forwarder != nil {
			pt := 102
			codecName := strings.ToUpper(strings.Split(slot.Codec.MimeType, "/")[1])
			mediaType := "video"
			if !slot.IsVideo {
				mediaType = "audio"
				switch slot.Codec.MimeType {
				case webrtc.MimeTypeOpus:
					pt = 111
				case webrtc.MimeTypePCMU:
					pt = 0
				default:
					log.Printf("HLSFeeder: Unsupported audio codec %s for slot %d", slot.Codec.MimeType, slot.Port)
					continue
				}
			}
			sdpContent.WriteString(fmt.Sprintf("m=%s %d RTP/AVP %d\n", mediaType, slot.Port, pt))
			sdpContent.WriteString(fmt.Sprintf("a=rtpmap:%d %s/%d", pt, codecName, slot.Codec.ClockRate))
			if !slot.IsVideo && slot.Codec.Channels == 2 {
				sdpContent.WriteString("/2")
			}
			sdpContent.WriteString("\n")

			if slot.IsVideo && slot.Codec.MimeType == webrtc.MimeTypeH264 {
				sdpContent.WriteString(fmt.Sprintf("a=fmtp:%d packetization-mode=1;profile-level-id=42e01f;level-asymmetry-allowed=1\n", pt))
			} else if !slot.IsVideo && slot.Codec.MimeType == webrtc.MimeTypeOpus {
				sdpContent.WriteString(fmt.Sprintf("a=fmtp:%d minptime=10;useinbandfec=1\n", pt))
			}
			activeMediaLines++
		}
	}

	if activeMediaLines == 0 {
		log.Println("HLSFeeder: No active tracks for SDP. Removing SDP file if it exists.")
		os.Remove(f.ffmpegSDPFile)
		return nil
	}

	log.Printf("HLSFeeder: Generated SDP for FFmpeg:\n%s", sdpContent.String())
	return os.WriteFile(f.ffmpegSDPFile, []byte(sdpContent.String()), 0644)
}

func (f *HLSFeeder) AddTrack(track *webrtc.TrackRemote, peerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.tracksCount >= maxHLSTracksDefault*2 {
		log.Printf("HLSFeeder: Max HLS tracks (%d) reached. Ignoring new track from peer %s.", maxHLSTracksDefault*2, peerID)
		return fmt.Errorf("max HLS tracks reached")
	}

	var targetSlot *HLSFeederSlot
	for _, slot := range f.slots {
		isKindMatch := (slot.IsVideo && track.Kind() == webrtc.RTPCodecTypeVideo) ||
			(!slot.IsVideo && track.Kind() == webrtc.RTPCodecTypeAudio)

		if slot.Forwarder == nil && isKindMatch {
			isPeerAlreadyUsingSlotOfKind := false
			for _, s_check := range f.slots {
				if s_check.Forwarder != nil && s_check.PeerID == peerID && s_check.IsVideo == slot.IsVideo {
					isPeerAlreadyUsingSlotOfKind = true
					break
				}
			}
			if !isPeerAlreadyUsingSlotOfKind {
				targetSlot = slot
				break
			}
		}
	}

	if targetSlot == nil {
		log.Printf("HLSFeeder: No available HLS slot for %s track from peer %s. Current tracks: %d.", track.Kind(), peerID, f.tracksCount)
		return fmt.Errorf("no available HLS slot for %s track from peer %s", track.Kind(), peerID)
	}

	forwarder, err := NewRTPForwarder(track, peerID, targetSlot.Port)
	if err != nil {
		return fmt.Errorf("create RTP forwarder for peer %s: %w", peerID, err)
	}

	targetSlot.Forwarder = forwarder
	targetSlot.PeerID = peerID
	actualCodec := track.Codec()
	targetSlot.Codec = actualCodec.RTPCodecCapability
	f.tracksCount++

	log.Printf("HLSFeeder: Added track from peer %s (%s, %s) to slot (port %d). Total HLS tracks: %d",
		peerID, track.ID(), track.Codec().MimeType, targetSlot.Port, f.tracksCount)

	forwarder.Start()

	if err := f.generateSDP(); err != nil {
		log.Printf("HLSFeeder: Error generating SDP after adding track: %v", err)
	}

	if !f.ffmpegRunning && f.tracksCount > 0 {
		go f.ensureFFmpegRunning()
	} else if f.ffmpegRunning {
		log.Println("HLSFeeder: FFmpeg running. SDP updated. Manual FFmpeg restart might be needed for significant input changes.")
	}
	return nil
}

func (f *HLSFeeder) RemoveTracksByPeer(peerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Printf("HLSFeeder: Removing tracks for peer %s.", peerID)
	removedCount := 0
	for _, slot := range f.slots {
		if slot.Forwarder != nil && slot.Forwarder.peerID == peerID {
			slot.Forwarder.Stop()
			slot.Forwarder = nil
			slot.PeerID = ""
			f.tracksCount--
			removedCount++
		}
	}

	if removedCount > 0 {
		log.Printf("HLSFeeder: Removed %d tracks for peer %s. Total HLS tracks: %d", removedCount, peerID, f.tracksCount)
		if err := f.generateSDP(); err != nil {
			log.Printf("HLSFeeder: Error generating SDP after removing tracks: %v", err)
		}
		if f.tracksCount == 0 && f.ffmpegRunning {
			f.signalStopFFmpeg()
		} else if f.ffmpegRunning {
			log.Println("HLSFeeder: FFmpeg running. SDP updated after track removal. Manual FFmpeg restart might be needed.")
		}
	}
}

func (f *HLSFeeder) ensureFFmpegRunning() {
	f.mu.Lock()
	if _, err := os.Stat(f.ffmpegSDPFile); os.IsNotExist(err) {
		log.Println("HLSFeeder: SDP file does not exist. FFmpeg will not be started.")
		f.mu.Unlock()
		return
	}
	if f.ffmpegRunning || f.tracksCount == 0 {
		f.mu.Unlock()
		return
	}
	f.ffmpegRunning = true
	f.stopFFmpegCmd = make(chan struct{})
	f.ffmpegWg.Add(1)
	f.mu.Unlock()

	log.Println("HLSFeeder: Attempting to start FFmpeg...")
	if err := f.startFFmpegInternal(); err != nil {
		log.Printf("HLSFeeder: Failed to start FFmpeg: %v", err)
		f.mu.Lock()
		f.ffmpegRunning = false
		if f.stopFFmpegCmd != nil {
			close(f.stopFFmpegCmd)
			f.stopFFmpegCmd = nil
		}
		f.ffmpegWg.Done()
		f.mu.Unlock()
	}
}

func (f *HLSFeeder) startFFmpegInternal() error {
	f.cleanupHLSOutputFiles()

	sdpBytes, err := os.ReadFile(f.ffmpegSDPFile)
	if err != nil {
		f.ffmpegWg.Done()
		return fmt.Errorf("read SDP file %s: %w", f.ffmpegSDPFile, err)
	}

	activeVideoMappings := []string{}
	activeAudioMappings := []string{}
	sdpInputIndex := 0
	scanner := bufio.NewScanner(strings.NewReader(string(sdpBytes)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "m=video") {
			activeVideoMappings = append(activeVideoMappings, fmt.Sprintf("[%d:v]", sdpInputIndex))
			sdpInputIndex++
		} else if strings.HasPrefix(line, "m=audio") {
			activeAudioMappings = append(activeAudioMappings, fmt.Sprintf("[%d:a]", sdpInputIndex))
			sdpInputIndex++
		}
	}

	if len(activeVideoMappings) == 0 && len(activeAudioMappings) == 0 {
		f.ffmpegWg.Done()
		return fmt.Errorf("no active video or audio streams in SDP for FFmpeg")
	}

	var complexFilter strings.Builder
	var outputVideoMap, outputAudioMap string

	if len(activeVideoMappings) > 0 {
		outputVideoMap = "[vout]"
		if len(activeVideoMappings) == 1 {
			complexFilter.WriteString(fmt.Sprintf("%sscale=640:360,setpts=PTS-STARTPTS[vout]", activeVideoMappings[0]))
		} else if len(activeVideoMappings) >= 2 {
			complexFilter.WriteString(fmt.Sprintf(
				"%sscale=320:240,setpts=PTS-STARTPTS[v0];%sscale=320:240,setpts=PTS-STARTPTS[v1];[v0][v1]xstack=inputs=2:layout=0_0|w0_0[vout]",
				activeVideoMappings[0], activeVideoMappings[1]))
			if len(activeVideoMappings) > 2 {
				log.Printf("HLSFeeder: More than 2 video tracks, HLS will only show first two combined.")
			}
		}
	}

	if len(activeAudioMappings) > 0 {
		outputAudioMap = "[aout]"
		if complexFilter.Len() > 0 {
			complexFilter.WriteString("; ")
		}
		audioInputsStr := strings.Join(activeAudioMappings, "")
		complexFilter.WriteString(fmt.Sprintf("%samix=inputs=%d:normalize=0[aout]", audioInputsStr, len(activeAudioMappings)))
	}

	ffmpegArgs := []string{
		"-protocol_whitelist", "file,udp,rtp",
		"-re",
		"-nostdin",
		"-i", f.ffmpegSDPFile,
		"-y",
	}

	if complexFilter.Len() > 0 {
		ffmpegArgs = append(ffmpegArgs, "-filter_complex", complexFilter.String())
	}

	if outputVideoMap != "" {
		ffmpegArgs = append(ffmpegArgs,
			"-map", outputVideoMap, "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
			"-pix_fmt", "yuv420p", "-g", "60", "-b:v", "800k", "-maxrate", "1000k", "-bufsize", "1500k",
		)
	}
	if outputAudioMap != "" {
		ffmpegArgs = append(ffmpegArgs,
			"-map", outputAudioMap, "-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "2",
		)
	}

	ffmpegArgs = append(ffmpegArgs,
		"-f", "hls", "-hls_time", "2", "-hls_list_size", "5",
		"-hls_flags", "delete_segments+omit_endlist",
		"-hls_segment_filename", filepath.Join(f.hlsOutputDir, "segment_%05d.ts"),
		filepath.Join(f.hlsOutputDir, "stream.m3u8"),
	)

	log.Printf("HLSFeeder: Starting FFmpeg with args: %s", strings.Join(ffmpegArgs, " "))

	f.mu.Lock()
	cmd := exec.Command("ffmpeg", ffmpegArgs...)
	f.ffmpegCmd = cmd
	currentStopChan := f.stopFFmpegCmd
	f.mu.Unlock()

	stderrPipe, _ := cmd.StderrPipe()
	stdoutPipe, _ := cmd.StdoutPipe()
	go pipeToLog("[FFMPEG Stderr]", stderrPipe)
	go pipeToLog("[FFMPEG Stdout]", stdoutPipe)

	if err := cmd.Start(); err != nil {
		f.ffmpegWg.Done()
		return fmt.Errorf("start FFmpeg process: %w", err)
	}
	log.Println("HLSFeeder: FFmpeg process started.")

	go func(currentCmd *exec.Cmd, stopSignal <-chan struct{}) {
		defer f.ffmpegWg.Done()
		processDone := make(chan error, 1)
		go func() {
			processDone <- currentCmd.Wait()
		}()

		select {
		case err := <-processDone:
			f.mu.Lock()
			f.ffmpegRunning = false
			if f.ffmpegCmd == currentCmd {
				f.ffmpegCmd = nil
			}
			if f.stopFFmpegCmd == stopSignal {
				f.stopFFmpegCmd = nil
			}
			f.mu.Unlock()
			if err != nil {
				log.Printf("HLSFeeder: FFmpeg process exited with error: %v", err)
			} else {
				log.Println("HLSFeeder: FFmpeg process exited gracefully.")
			}

			f.mu.Lock()
			tracksExist := f.tracksCount > 0
			f.mu.Unlock()

			if tracksExist {
				log.Println("HLSFeeder: FFmpeg exited but tracks still active. Attempting restart after delay.")
				time.Sleep(ffmpegRestartDelay)
				f.ensureFFmpegRunning()
			} else {
				time.Sleep(ffmpegOutputCleanupDelay)
				f.cleanupHLSOutputFiles()
			}
		case <-stopSignal:
			log.Println("HLSFeeder: Received signal to stop FFmpeg. Sending interrupt.")
			if err := currentCmd.Process.Signal(os.Interrupt); err != nil {
				log.Printf("HLSFeeder: Error sending SIGINT to FFmpeg, attempting kill: %v", err)
				currentCmd.Process.Kill()
			}
			<-processDone
			f.mu.Lock()
			f.ffmpegRunning = false
			if f.ffmpegCmd == currentCmd {
				f.ffmpegCmd = nil
			}
			if f.stopFFmpegCmd == stopSignal {
				f.stopFFmpegCmd = nil
			}
			f.mu.Unlock()
			log.Println("HLSFeeder: FFmpeg process stopped via signal.")
			time.Sleep(ffmpegOutputCleanupDelay)
			f.cleanupHLSOutputFiles()
		}
	}(cmd, currentStopChan)
	return nil
}

func (f *HLSFeeder) cleanupHLSOutputFiles() {
	files, err := filepath.Glob(filepath.Join(f.hlsOutputDir, "*.ts"))
	if err == nil {
		for _, file := range files {
			os.Remove(file)
		}
	}
	filesM3U8, err := filepath.Glob(filepath.Join(f.hlsOutputDir, "*.m3u8"))
	if err == nil {
		for _, file := range filesM3U8 {
			os.Remove(file)
		}
	}
	log.Printf("HLSFeeder: Cleaned HLS output directory: %s", f.hlsOutputDir)
}

func (f *HLSFeeder) signalStopFFmpeg() {
	if f.stopFFmpegCmd != nil {
		select {
		case <-f.stopFFmpegCmd:
		default:
			close(f.stopFFmpegCmd)
			log.Println("HLSFeeder: Signaling FFmpeg process to stop...")
		}
	} else {
		f.ffmpegRunning = false
		f.ffmpegCmd = nil
	}
}

func (f *HLSFeeder) Stop() {
	f.mu.Lock()
	log.Println("HLSFeeder: Stopping...")
	for _, slot := range f.slots {
		if slot.Forwarder != nil {
			slot.Forwarder.Stop()
			slot.Forwarder = nil
			slot.PeerID = ""
		}
	}
	f.tracksCount = 0
	f.signalStopFFmpeg()
	f.mu.Unlock()

	f.ffmpegWg.Wait()
	log.Println("HLSFeeder: Stopped.")
}

func (f *HLSFeeder) Start() error {
	log.Println("HLSFeeder: Initialized. FFmpeg will start when tracks are added.")
	return nil
}

func pipeToLog(prefix string, pipe io.ReadCloser) {
	defer pipe.Close()
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		log.Printf("%s %s", prefix, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Printf("%s Pipe error: %v", prefix, err)
	}
}
