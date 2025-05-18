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
		return nil, fmt.Errorf("dial UDP for RTP forward to %s for peer %s track %s: %w", destAddr, peerID, track.ID(), err)
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

		log.Printf("Peer %s: Started RTP forwarder for track %s (%s) to port %d", f.peerID, f.track.ID(), f.track.Codec().MimeType, f.rtpPort)
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
					} else if !strings.Contains(err.Error(), "use of closed network connection") && !strings.Contains(err.Error(), "RTPReceiver already closed") {
						log.Printf("Peer %s: Error reading from track %s: %v", f.peerID, f.track.ID(), err)
					}
					return
				}
				if _, err := f.conn.Write(b[:n]); err != nil {
					if !strings.Contains(err.Error(), "use of closed network connection") {
						log.Printf("Peer %s: Error writing RTP packet to UDP port %d for track %s: %v", f.peerID, f.rtpPort, f.track.ID(), err)
					}
				}
			}
		}
	}()
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
		log.Printf("HLSFeeder: No available HLS slot for %s track from peer %s (or peer already has this kind). Current total HLS tracks: %d.", track.Kind(), peerID, f.tracksCount)
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

	currentFFmpegRunning := f.ffmpegRunning
	currentTracksCount := f.tracksCount
	f.mu.Unlock()

	forwarder.Start()

	if err := f.generateSDP(); err != nil {
		log.Printf("HLSFeeder: Error generating SDP after adding track for peer %s: %v", peerID, err)
	}

	if !currentFFmpegRunning && currentTracksCount > 0 {
		log.Printf("HLSFeeder: FFmpeg not running and tracks > 0. Attempting to start FFmpeg.")
		f.ensureFFmpegRunning()
	} else if currentFFmpegRunning {
		log.Println("HLSFeeder: FFmpeg already running. SDP updated. Manual FFmpeg restart might be needed if input sources changed significantly.")
	}
	return nil
}

func (f *HLSFeeder) RemoveTracksByPeer(peerID string) {
	f.mu.Lock()

	log.Printf("HLSFeeder: Attempting to remove tracks for peer %s.", peerID)
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
		log.Printf("HLSFeeder: Removed %d HLS tracks for peer %s. Total HLS tracks remaining: %d", removedCount, peerID, f.tracksCount)

		f.mu.Unlock()
		if err := f.generateSDP(); err != nil {
			log.Printf("HLSFeeder: Error generating SDP after removing tracks for peer %s: %v", peerID, err)
		}
		f.mu.Lock()

		if f.tracksCount == 0 && f.ffmpegRunning {
			log.Println("HLSFeeder: All HLS tracks removed and FFmpeg is running. Signaling FFmpeg to stop.")
			f.signalStopFFmpeg()
		} else if f.ffmpegRunning {
			fmt.Printf("HLSFeeder: FFmpeg running. SDP updated after track removal for peer %s. Manual FFmpeg restart might be needed.", peerID)
		}
	}
	f.mu.Unlock()
}

func (f *HLSFeeder) ensureFFmpegRunning() {
	f.mu.Lock()
	if f.ffmpegRunning {
		log.Printf("HLSFeeder: ensureFFmpegRunning - FFmpeg already running. Tracks: %d", f.tracksCount)
		f.mu.Unlock()
		return
	}

	if f.tracksCount == 0 {
		log.Printf("HLSFeeder: ensureFFmpegRunning - No active tracks. FFmpeg will not be started.")
		f.mu.Unlock()
		return
	}

	sdpFi, err := os.Stat(f.ffmpegSDPFile)
	if os.IsNotExist(err) {
		log.Printf("HLSFeeder: ensureFFmpegRunning - SDP file (%s) does not exist. FFmpeg will not be started.", f.ffmpegSDPFile)
		f.mu.Unlock()
		return
	}
	if err != nil {
		log.Printf("HLSFeeder: ensureFFmpegRunning - Error stating SDP file (%s): %v. FFmpeg will not be started.", f.ffmpegSDPFile, err)
		f.mu.Unlock()
		return
	}
	if sdpFi.Size() == 0 {
		log.Printf("HLSFeeder: ensureFFmpegRunning - SDP file (%s) is empty. FFmpeg will not be started.", f.ffmpegSDPFile)
		f.mu.Unlock()
		return
	}

	log.Println("HLSFeeder: Conditions met to start FFmpeg. Setting ffmpegRunning=true and adding to WaitGroup.")
	f.ffmpegRunning = true
	if f.stopFFmpegCmd != nil {
		select {
		case <-f.stopFFmpegCmd:
		default:
			close(f.stopFFmpegCmd)
		}
	}
	f.stopFFmpegCmd = make(chan struct{})
	f.ffmpegWg.Add(1)

	err = f.startFFmpegInternal()
	if err != nil {
		log.Printf("HLSFeeder: Failed to start FFmpeg process: %v", err)
		f.ffmpegRunning = false
		if f.stopFFmpegCmd != nil {
			select {
			case <-f.stopFFmpegCmd:
			default:
				close(f.stopFFmpegCmd)
			}
			f.stopFFmpegCmd = nil
		}
		f.ffmpegWg.Done()
	} else {
		log.Println("HLSFeeder: FFmpeg process initiation successful via ensureFFmpegRunning.")
	}
	f.mu.Unlock()
}

func (f *HLSFeeder) startFFmpegInternal() error {
	f.cleanupHLSOutputFiles()

	sdpBytes, err := os.ReadFile(f.ffmpegSDPFile)
	if err != nil {
		return fmt.Errorf("internal error: failed to read SDP file %s in startFFmpegInternal: %w", f.ffmpegSDPFile, err)
	}
	log.Printf("HLSFeeder: SDP file content for FFmpeg at start:\n%s", string(sdpBytes))

	activeVideoMappings := []string{}
	activeAudioMappings := []string{}
	ffmpegInputIndex := 0
	scanner := bufio.NewScanner(strings.NewReader(string(sdpBytes)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "m=video") {
			activeVideoMappings = append(activeVideoMappings, fmt.Sprintf("[%d:v:0]", ffmpegInputIndex))
			ffmpegInputIndex++
		} else if strings.HasPrefix(line, "m=audio") {
			activeAudioMappings = append(activeAudioMappings, fmt.Sprintf("[%d:a:0]", ffmpegInputIndex))
			ffmpegInputIndex++
		}
	}

	if len(activeVideoMappings) == 0 && len(activeAudioMappings) == 0 {
		return fmt.Errorf("internal error: no active video or audio m-lines in SDP for FFmpeg")
	}
	log.Printf("HLSFeeder: FFmpeg mappings - Video: %v, Audio: %v", activeVideoMappings, activeAudioMappings)

	var complexFilter strings.Builder
	var outputVideoMap, outputAudioMap string

	if len(activeVideoMappings) > 0 {
		outputVideoMap = "[vout]"
		if len(activeVideoMappings) == 1 {
			complexFilter.WriteString(fmt.Sprintf("%sscale=640:360,setpts=PTS-STARTPTS%s", activeVideoMappings[0], outputVideoMap))
		} else if len(activeVideoMappings) >= 2 {
			complexFilter.WriteString(fmt.Sprintf(
				"%sscale=320:240,setpts=PTS-STARTPTS[v0];%sscale=320:240,setpts=PTS-STARTPTS[v1];[v0][v1]xstack=inputs=2:layout=0_0|w0_0%s",
				activeVideoMappings[0], activeVideoMappings[1], outputVideoMap))
			if len(activeVideoMappings) > 2 {
				log.Printf("HLSFeeder: More than 2 video tracks (%d), HLS will only show first two combined.", len(activeVideoMappings))
			}
		}
	}

	if len(activeAudioMappings) > 0 {
		outputAudioMap = "[aout]"
		if complexFilter.Len() > 0 {
			complexFilter.WriteString("; ")
		}
		audioInputsStr := ""
		for _, amap := range activeAudioMappings {
			audioInputsStr += amap
		}
		complexFilter.WriteString(fmt.Sprintf("%samix=inputs=%d:normalize=0%s", audioInputsStr, len(activeAudioMappings), outputAudioMap))
	}

	if err := os.MkdirAll(f.hlsOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create HLS output directory %s for FFmpeg: %w", f.hlsOutputDir, err)
	}

	ffmpegArgs := []string{
		"-protocol_whitelist", "file,udp,rtp",
		"-nostdin",
		"-analyzeduration", "5000000",
		"-probesize", "5000000",
		"-i", f.ffmpegSDPFile,
		"-y",
	}

	if complexFilter.Len() > 0 {
		log.Printf("HLSFeeder: Using FFmpeg filter_complex: %s", complexFilter.String())
		ffmpegArgs = append(ffmpegArgs, "-filter_complex", complexFilter.String())
	}

	if outputVideoMap != "" {
		ffmpegArgs = append(ffmpegArgs, "-map", outputVideoMap)
		ffmpegArgs = append(ffmpegArgs,
			"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
			"-pix_fmt", "yuv420p", "-r", "25", "-g", "50",
			"-b:v", "800k", "-maxrate", "1000k", "-bufsize", "1500k",
		)
	} else if len(activeVideoMappings) == 1 && complexFilter.Len() == 0 {
		ffmpegArgs = append(ffmpegArgs, "-map", activeVideoMappings[0])
		ffmpegArgs = append(ffmpegArgs,
			"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
			"-pix_fmt", "yuv420p", "-r", "25", "-g", "50",
			"-b:v", "800k", "-maxrate", "1000k", "-bufsize", "1500k",
		)
	}

	if outputAudioMap != "" {
		ffmpegArgs = append(ffmpegArgs, "-map", outputAudioMap)
		ffmpegArgs = append(ffmpegArgs,
			"-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "2",
		)
	} else if len(activeAudioMappings) == 1 && complexFilter.Len() == 0 {
		ffmpegArgs = append(ffmpegArgs, "-map", activeAudioMappings[0])
		ffmpegArgs = append(ffmpegArgs,
			"-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "2",
		)
	}

	if (outputVideoMap == "" && (len(activeVideoMappings) != 1 || complexFilter.Len() != 0)) &&
		(outputAudioMap == "" && (len(activeAudioMappings) != 1 || complexFilter.Len() != 0)) {
		if len(activeVideoMappings) == 0 && len(activeAudioMappings) == 0 {
			return fmt.Errorf("FFmpeg: no video or audio outputs to map, this is an internal logic error")
		}
	}

	ffmpegArgs = append(ffmpegArgs,
		"-f", "hls",
		"-hls_time", "2",
		"-hls_list_size", "5",
		"-hls_flags", "delete_segments+omit_endlist",
		"-hls_segment_filename", filepath.Join(f.hlsOutputDir, "segment_%05d.ts"),
		filepath.Join(f.hlsOutputDir, "stream.m3u8"),
	)

	log.Printf("HLSFeeder: Starting FFmpeg with command: ffmpeg %s", strings.Join(ffmpegArgs, " "))

	cmd := exec.Command("ffmpeg", ffmpegArgs...)
	f.ffmpegCmd = cmd
	currentStopChan := f.stopFFmpegCmd

	stderrPipe, _ := cmd.StderrPipe()
	stdoutPipe, _ := cmd.StdoutPipe()
	go pipeToLog("[FFMPEG Stderr]", stderrPipe)
	go pipeToLog("[FFMPEG Stdout]", stdoutPipe)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start FFmpeg process: %w", err)
	}
	log.Println("HLSFeeder: FFmpeg process started successfully.")

	go func(monitoredCmd *exec.Cmd, stopSignal <-chan struct{}) {
		defer func() {
			log.Println("HLSFeeder: FFmpeg monitoring goroutine exiting, calling f.ffmpegWg.Done()")
			f.ffmpegWg.Done()
		}()

		processDone := make(chan error, 1)
		go func() {
			processDone <- monitoredCmd.Wait()
		}()

		select {
		case err := <-processDone:
			f.mu.Lock()
			f.ffmpegRunning = false
			if f.ffmpegCmd == monitoredCmd {
				f.ffmpegCmd = nil
			}
			if f.stopFFmpegCmd == stopSignal {
			}
			f.mu.Unlock()

			if err != nil {
				log.Printf("HLSFeeder: FFmpeg process exited with error: %v", err)
			} else {
				log.Println("HLSFeeder: FFmpeg process exited gracefully.")
			}

			f.mu.Lock()
			tracksStillExist := f.tracksCount > 0
			f.mu.Unlock()

			if tracksStillExist {
				log.Println("HLSFeeder: FFmpeg exited, but tracks are still active. Attempting restart after delay.")
				time.Sleep(ffmpegRestartDelay)
				f.ensureFFmpegRunning()
			} else {
				log.Println("HLSFeeder: FFmpeg exited and no active tracks. Cleaning up HLS output files.")
				time.Sleep(ffmpegOutputCleanupDelay)
				f.cleanupHLSOutputFiles()
			}

		case <-stopSignal:
			log.Printf("HLSFeeder: Received signal to stop FFmpeg (PID: %d). Sending interrupt.", monitoredCmd.Process.Pid)
			if monitoredCmd.Process == nil {
				log.Println("HLSFeeder: FFmpeg process was nil when stop signal received.")
			} else if err := monitoredCmd.Process.Signal(os.Interrupt); err != nil {
				log.Printf("HLSFeeder: Error sending SIGINT to FFmpeg (PID: %d), attempting kill: %v", monitoredCmd.Process.Pid, err)
				if monitoredCmd.Process != nil {
					monitoredCmd.Process.Kill()
				}
			}

			select {
			case err := <-processDone:
				if err != nil {
					log.Printf("HLSFeeder: FFmpeg (PID: %d) exited after interrupt with error: %v", monitoredCmd.Process.Pid, err)
				} else {
					log.Printf("HLSFeeder: FFmpeg (PID: %d) exited gracefully after interrupt.", monitoredCmd.Process.Pid)
				}
			case <-time.After(ffmpegStopTimeout):
				log.Printf("HLSFeeder: FFmpeg (PID: %d) did not stop within %s after interrupt, attempting kill again.", monitoredCmd.Process.Pid, ffmpegStopTimeout)
				if monitoredCmd.Process != nil {
					monitoredCmd.Process.Kill()
					<-processDone
				}
			}

			f.mu.Lock()
			f.ffmpegRunning = false
			if f.ffmpegCmd == monitoredCmd {
				f.ffmpegCmd = nil
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
	if f.ffmpegCmd == nil || !f.ffmpegRunning {
		log.Println("HLSFeeder: signalStopFFmpeg called, but FFmpeg not running or command is nil.")
		f.ffmpegRunning = false
		return
	}

	if f.stopFFmpegCmd != nil {
		log.Println("HLSFeeder: Signaling current FFmpeg process to stop...")
		select {
		case <-f.stopFFmpegCmd:
			log.Println("HLSFeeder: stopFFmpegCmd was already closed.")
		default:
			close(f.stopFFmpegCmd)
		}
	} else {
		log.Println("HLSFeeder: signalStopFFmpeg: stopFFmpegCmd is nil, FFmpeg might be stopping or in inconsistent state.")
		if f.ffmpegCmd != nil && f.ffmpegCmd.Process != nil {
			log.Println("HLSFeeder: Fallback: Attempting to kill FFmpeg process directly.")
			f.ffmpegCmd.Process.Kill()
		}
		f.ffmpegRunning = false
	}
}

func (f *HLSFeeder) Stop() {
	f.mu.Lock()
	log.Println("HLSFeeder: Stopping all operations...")
	for _, slot := range f.slots {
		if slot.Forwarder != nil {
			slot.Forwarder.Stop()
			slot.Forwarder = nil
			slot.PeerID = ""
		}
	}
	f.tracksCount = 0

	if f.ffmpegRunning {
		f.signalStopFFmpeg()
	}
	f.mu.Unlock()

	log.Println("HLSFeeder: Waiting for FFmpeg process to terminate...")
	f.ffmpegWg.Wait()

	f.cleanupHLSOutputFiles()
	if _, err := os.Stat(f.hlsOutputDir); !os.IsNotExist(err) {
		os.Remove(f.ffmpegSDPFile)
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
		if err != io.EOF && !strings.Contains(err.Error(), "file already closed") {
			log.Printf("%s Pipe error: %v", prefix, err)
		}
	}
}
