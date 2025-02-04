/*
Package server is the place we integrate the Livepeer node with the LPMS media server.
*/
package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/go-livepeer/eth"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"

	"github.com/cenkalti/backoff"
	"github.com/ericxtang/m3u8"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/core"
	lpmscore "github.com/livepeer/lpms/core"
	ffmpeg "github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/segmenter"
	"github.com/livepeer/lpms/stream"
	"github.com/livepeer/lpms/vidplayer"
)

var ErrAlreadyExists = errors.New("StreamAlreadyExists")
var ErrBroadcast = errors.New("ErrBroadcast")
var ErrLowDeposit = errors.New("ErrLowDeposit")
var ErrStorage = errors.New("ErrStorage")
var ErrDiscovery = errors.New("ErrDiscovery")
var ErrNoOrchs = errors.New("ErrNoOrchs")
var ErrUnknownStream = errors.New("ErrUnknownStream")

const HLSWaitInterval = time.Second
const HLSBufferCap = uint(43200) //12 hrs assuming 1s segment
const HLSBufferWindow = uint(5)
const StreamKeyBytes = 6

const SegLen = 2 * time.Second
const BroadcastRetry = 15 * time.Second

var BroadcastPrice = big.NewInt(1)
var BroadcastJobVideoProfiles = []ffmpeg.VideoProfile{ffmpeg.P240p30fps4x3, ffmpeg.P360p30fps16x9}

type rtmpConnection struct {
	mid     core.ManifestID
	nonce   uint64
	stream  stream.RTMPVideoStream
	pl      core.PlaylistManager
	profile *ffmpeg.VideoProfile

	needOrch chan struct{}
	eof      chan struct{}

	// Thread sensitive fields. All accesses to the
	// following fields should be protected by `lock`
	sess *BroadcastSession
	lock *sync.RWMutex
}

type LivepeerServer struct {
	RTMPSegmenter lpmscore.RTMPSegmenter
	LPMS          *lpmscore.LPMS
	LivepeerNode  *core.LivepeerNode
	HttpMux       *http.ServeMux

	ExposeCurrentManifest bool

	// Thread sensitive fields. All accesses to the
	// following fields should be protected by `connectionLock`
	rtmpConnections map[core.ManifestID]*rtmpConnection
	lastHLSStreamID core.StreamID
	lastManifestID  core.ManifestID
	connectionLock  *sync.RWMutex
}

func NewLivepeerServer(rtmpAddr string, httpAddr string, lpNode *core.LivepeerNode) *LivepeerServer {
	opts := lpmscore.LPMSOpts{
		RtmpAddr: rtmpAddr, RtmpDisabled: true,
		HttpAddr: httpAddr,
		WorkDir:  lpNode.WorkDir,
	}
	switch lpNode.NodeType {
	case core.BroadcasterNode:
		opts.RtmpDisabled = false
	case core.OrchestratorNode:
		opts.HttpMux = http.NewServeMux()
	}
	server := lpmscore.New(&opts)
	return &LivepeerServer{RTMPSegmenter: server, LPMS: server, LivepeerNode: lpNode, HttpMux: opts.HttpMux, connectionLock: &sync.RWMutex{}, rtmpConnections: make(map[core.ManifestID]*rtmpConnection)}
}

//StartServer starts the LPMS server
func (s *LivepeerServer) StartMediaServer(ctx context.Context, maxPricePerSegment *big.Int, transcodingOptions string) error {
	if s.LivepeerNode.Eth != nil {
		BroadcastPrice = maxPricePerSegment
		glog.V(common.SHORT).Infof("Transcode Job Price: %v", eth.FormatUnits(BroadcastPrice, "ETH"))
	}

	bProfiles := make([]ffmpeg.VideoProfile, 0)
	for _, opt := range strings.Split(transcodingOptions, ",") {
		p, ok := ffmpeg.VideoProfileLookup[strings.TrimSpace(opt)]
		if ok {
			bProfiles = append(bProfiles, p)
		}
	}
	BroadcastJobVideoProfiles = bProfiles

	glog.V(common.SHORT).Infof("Transcode Job Type: %v", BroadcastJobVideoProfiles)

	//LPMS handlers for handling RTMP video
	s.LPMS.HandleRTMPPublish(createRTMPStreamIDHandler(s), gotRTMPStreamHandler(s), endRTMPStreamHandler(s))
	s.LPMS.HandleRTMPPlay(getRTMPStreamHandler(s))

	//LPMS hanlder for handling HLS video play
	s.LPMS.HandleHLSPlay(getHLSMasterPlaylistHandler(s), getHLSMediaPlaylistHandler(s), getHLSSegmentHandler(s))

	//Start the LPMS server
	lpmsCtx, cancel := context.WithCancel(context.Background())
	ec := make(chan error, 1)
	go func() {
		if err := s.LPMS.Start(lpmsCtx); err != nil {
			// typically triggered if there's an error with broadcaster LPMS
			// transcoder LPMS should return without an error
			ec <- s.LPMS.Start(lpmsCtx)
		}
	}()

	select {
	case err := <-ec:
		glog.Infof("LPMS Server Error: %v.  Quitting...", err)
		cancel()
		return err
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	}
}

//RTMP Publish Handlers
func createRTMPStreamIDHandler(s *LivepeerServer) func(url *url.URL) (strmID string) {
	return func(url *url.URL) (strmID string) {
		//Create a ManifestID
		//If manifestID is passed in, use that one
		//Else create one
		mid := parseManifestID(url.Query().Get("manifestID"))
		if mid == "" {
			mid = core.RandomManifestID()
		}

		// Ensure there's no concurrent StreamID with the same name
		s.connectionLock.RLock()
		defer s.connectionLock.RUnlock()
		if _, exists := s.rtmpConnections[mid]; exists {
			glog.Error("Manifest already exists ", mid)
			return ""
		}

		// Generate RTMP part of StreamID
		key := hex.EncodeToString(core.RandomIdGenerator(StreamKeyBytes))
		return core.MakeStreamIDFromString(string(mid), key).String()
	}

}

func rtmpManifestID(rtmpStrm stream.RTMPVideoStream) core.ManifestID {
	return parseManifestID(rtmpStrm.GetStreamID())
}

func gotRTMPStreamHandler(s *LivepeerServer) func(url *url.URL, rtmpStrm stream.RTMPVideoStream) (err error) {
	return func(url *url.URL, rtmpStrm stream.RTMPVideoStream) (err error) {

		cxn, err := s.registerConnection(rtmpStrm)
		if err != nil {
			return err
		}

		mid := cxn.mid
		nonce := cxn.nonce
		startSeq := 0

		streamStarted := false
		//Segment the stream, insert the segments into the broadcaster
		go func(rtmpStrm stream.RTMPVideoStream) {
			hid := string(core.RandomManifestID()) // ffmpeg m3u8 output name
			hlsStrm := stream.NewBasicHLSVideoStream(hid, stream.DefaultHLSStreamWin)
			hlsStrm.SetSubscriber(func(seg *stream.HLSSegment, eof bool) {
				if eof {
					// XXX update HLS manifest
					return
				}
				if streamStarted == false {
					streamStarted = true
					if monitor.Enabled {
						monitor.LogStreamStartedEvent(nonce)
					}
				}
				processSegment(cxn, seg)
			})

			segOptions := segmenter.SegmenterOptions{
				StartSeq:  startSeq,
				SegLength: SegLen,
			}
			err := s.RTMPSegmenter.SegmentRTMPToHLS(context.Background(), rtmpStrm, hlsStrm, segOptions)
			if err != nil {
				// Stop the incoming RTMP connection.
				// TODO retry segmentation if err != SegmenterTimeout; may be recoverable
				rtmpStrm.Close()
			}

		}(rtmpStrm)

		if monitor.Enabled {
			monitor.LogStreamCreatedEvent(string(mid), nonce)
		}

		glog.Infof("\n\nVideo Created With ManifestID: %v\n\n", mid)

		//Create Transcode Job Onchain
		go s.startSessionListener(cxn)
		cxn.needOrch <- struct{}{}
		return nil
	}
}

func endRTMPStreamHandler(s *LivepeerServer) func(url *url.URL, rtmpStrm stream.RTMPVideoStream) error {
	return func(url *url.URL, rtmpStrm stream.RTMPVideoStream) error {
		mid := rtmpManifestID(rtmpStrm)
		//Remove RTMP stream
		s.connectionLock.Lock()
		defer s.connectionLock.Unlock()
		cxn, ok := s.rtmpConnections[mid]
		if !ok || cxn.pl == nil {
			glog.Error("Attempted to end unknown stream with manifest ID ", mid)
			return ErrUnknownStream
		}
		cxn.pl.Cleanup()
		if monitor.Enabled {
			monitor.LogStreamEndedEvent(cxn.nonce)
		}
		cxn.eof <- struct{}{}
		delete(s.rtmpConnections, mid)

		return nil
	}
}

func (s *LivepeerServer) registerConnection(rtmpStrm stream.RTMPVideoStream) (*rtmpConnection, error) {
	nonce := rand.Uint64()

	// If running in on-chain mode, check for a reasonable deposit
	if s.LivepeerNode.Eth != nil {
		sender, err := s.LivepeerNode.Eth.Senders(s.LivepeerNode.Eth.Account().Address)
		if err != nil {
			return nil, err
		}

		if sender.Deposit.Cmp(big.NewInt(0)) <= 0 {
			glog.Errorf("No deposit - cannot start broadcast session")

			if monitor.Enabled {
				monitor.LogStreamCreateFailed(nonce, "LowDeposit")
			}

			return nil, ErrLowDeposit
		}
	}

	// Set up the connection tracking
	mid := rtmpManifestID(rtmpStrm)
	if drivers.NodeStorage == nil {
		glog.Error("Missing node storage")
		return nil, ErrStorage
	}
	storage := drivers.NodeStorage.NewSession(string(mid))
	// Build the source video profile from the RTMP stream.
	resolution := fmt.Sprintf("%vx%v", rtmpStrm.Width(), rtmpStrm.Height())
	vProfile := ffmpeg.VideoProfile{
		Name:       "source",
		Resolution: resolution,
		Bitrate:    "4000k", // Fix this
	}
	hlsStrmID := core.MakeStreamID(mid, &vProfile)
	s.connectionLock.Lock()
	defer s.connectionLock.Unlock()
	_, exists := s.rtmpConnections[mid]
	if exists {
		// We can only have one concurrent stream per ManifestID
		return nil, ErrAlreadyExists
	}
	cxn := &rtmpConnection{
		mid:     mid,
		nonce:   nonce,
		stream:  rtmpStrm,
		pl:      core.NewBasicPlaylistManager(mid, storage),
		profile: &vProfile,
		lock:    &sync.RWMutex{},

		needOrch: make(chan struct{}),
		eof:      make(chan struct{}),
	}
	s.rtmpConnections[mid] = cxn
	s.lastManifestID = mid
	s.lastHLSStreamID = hlsStrmID

	return cxn, nil
}

func (s *LivepeerServer) startSession(cxn *rtmpConnection) *BroadcastSession {

	mid := cxn.mid
	cpl := cxn.pl
	var sess *BroadcastSession

	// Connect to the orchestrator. If it fails, retry for as long
	// as the RTMP stream is alive
	broadcastFunc := func() error {
		var err error
		sess, err = selectOrchestrator(s.LivepeerNode, cpl)
		if err == ErrDiscovery {
			return nil // discovery disabled, don't retry
		} else if err != nil {
			// Should be logged upstream
		}
		s.connectionLock.RLock()
		defer s.connectionLock.RUnlock()
		_, active := s.rtmpConnections[mid]
		if active {
			return err
		}
		return nil // stop if inactive
	}
	expb := backoff.NewExponentialBackOff()
	expb.MaxInterval = BroadcastRetry
	expb.MaxElapsedTime = 0
	backoff.Retry(broadcastFunc, expb)
	return sess
}

func (s *LivepeerServer) startSessionListener(cxn *rtmpConnection) {
	mut := cxn.lock
	sem := make(chan struct{}, 1)
	runStartSession := func() {
		// Quickly lock-unlock to avoid blocking longer than necessary
		mut.Lock()
		cxn.sess = nil
		mut.Unlock()

		sess := s.startSession(cxn) // this could take awhile

		// Retain the connectionLock for the rest of the function to ensure
		// we don't terminate the stream *then* assign the session to the cxn
		s.connectionLock.RLock()
		defer s.connectionLock.RUnlock()
		if _, active := s.rtmpConnections[cxn.mid]; !active {
			sess = nil // don't assign if stream terminated
		}
		mut.Lock()
		defer mut.Unlock()
		cxn.sess = sess
	}
	glog.V(common.DEBUG).Info("Starting broadcast listener for ", cxn.mid)
	finished := false
	for {
		if finished {
			break
		}
		select {

		case <-cxn.needOrch:
			go func() {
				select {
				case sem <- struct{}{}:
					runStartSession()
					<-sem
				default:
					break
				}
			}()

		case <-cxn.eof:
			finished = true
			break
		}
	}
	glog.V(common.DEBUG).Info("Stopping broadcast listener for ", cxn.mid)
}

//End RTMP Publish Handlers

//HLS Play Handlers
func getHLSMasterPlaylistHandler(s *LivepeerServer) func(url *url.URL) (*m3u8.MasterPlaylist, error) {
	return func(url *url.URL) (*m3u8.MasterPlaylist, error) {
		var manifestID core.ManifestID
		if s.ExposeCurrentManifest && "/stream/current.m3u8" == strings.ToLower(url.Path) {
			manifestID = s.LastManifestID()
		} else {
			sid := parseStreamID(url.Path)
			if sid.Rendition != "" {
				// requesting a media PL, not master PL
				return nil, vidplayer.ErrNotFound
			}
			manifestID = sid.ManifestID
		}

		s.connectionLock.RLock()
		defer s.connectionLock.RUnlock()
		cxn, ok := s.rtmpConnections[manifestID]
		if !ok || cxn.pl == nil {
			return nil, vidplayer.ErrNotFound
		}
		cpl := cxn.pl

		if cpl.ManifestID() != manifestID {
			return nil, vidplayer.ErrNotFound
		}
		return cpl.GetHLSMasterPlaylist(), nil
	}
}

func getHLSMediaPlaylistHandler(s *LivepeerServer) func(url *url.URL) (*m3u8.MediaPlaylist, error) {
	return func(url *url.URL) (*m3u8.MediaPlaylist, error) {
		strmID := parseStreamID(url.Path)
		mid := strmID.ManifestID
		s.connectionLock.RLock()
		defer s.connectionLock.RUnlock()
		cxn, ok := s.rtmpConnections[mid]
		if !ok || cxn.pl == nil {
			return nil, vidplayer.ErrNotFound
		}

		//Get the hls playlist
		pl := cxn.pl.GetHLSMediaPlaylist(strmID.Rendition)
		if pl == nil {
			return nil, vidplayer.ErrNotFound
		}
		return pl, nil
	}
}

func getHLSSegmentHandler(s *LivepeerServer) func(url *url.URL) ([]byte, error) {
	return func(url *url.URL) ([]byte, error) {
		// Strip the /stream/ prefix
		segName := cleanStreamPrefix(url.Path)
		if segName == "" || drivers.NodeStorage == nil {
			glog.Error("SegName not found or storage nil")
			return nil, vidplayer.ErrNotFound
		}
		parts := strings.SplitN(segName, "/", 2)
		if len(parts) <= 0 {
			glog.Error("Unexpected path structure")
			return nil, vidplayer.ErrNotFound
		}
		memoryOS, ok := drivers.NodeStorage.(*drivers.MemoryOS)
		if !ok {
			return nil, vidplayer.ErrNotFound
		}
		// We index the session by the first entry of the path, eg
		// <session>/<more-path>/<data>
		os := memoryOS.GetSession(parts[0])
		if os == nil {
			return nil, vidplayer.ErrNotFound
		}
		data := os.GetData(segName)
		if len(data) > 0 {
			return data, nil
		}
		return nil, vidplayer.ErrNotFound
	}
}

//End HLS Play Handlers

//Start RTMP Play Handlers
func getRTMPStreamHandler(s *LivepeerServer) func(url *url.URL) (stream.RTMPVideoStream, error) {
	return func(url *url.URL) (stream.RTMPVideoStream, error) {
		mid := parseManifestID(url.Path)
		s.connectionLock.RLock()
		cxn, ok := s.rtmpConnections[mid]
		defer s.connectionLock.RUnlock()
		if !ok {
			glog.Error("Cannot find RTMP stream for ManifestID ", mid)
			return nil, vidplayer.ErrNotFound
		}

		//Could use a subscriber, but not going to here because the RTMP stream doesn't need to be available for consumption by multiple views.  It's only for the segmenter.
		return cxn.stream, nil
	}
}

//End RTMP Handlers

//Helper Methods Begin

// Match all leading spaces, slashes and optionally `stream/`
var StreamPrefix = regexp.MustCompile(`^[ /]*(stream/)?`)

func cleanStreamPrefix(reqPath string) string {
	return StreamPrefix.ReplaceAllString(reqPath, "")
}

func parseStreamID(reqPath string) core.StreamID {
	// remove extension and create streamid
	p := strings.TrimSuffix(reqPath, path.Ext(reqPath))
	return core.SplitStreamIDString(cleanStreamPrefix(p))
}

func parseManifestID(reqPath string) core.ManifestID {
	return parseStreamID(reqPath).ManifestID
}

func (s *LivepeerServer) LastManifestID() core.ManifestID {
	s.connectionLock.RLock()
	defer s.connectionLock.RUnlock()
	return s.lastManifestID
}

func (s *LivepeerServer) LastHLSStreamID() core.StreamID {
	s.connectionLock.RLock()
	defer s.connectionLock.RUnlock()
	return s.lastHLSStreamID
}

func (s *LivepeerServer) GetNodeStatus() *net.NodeStatus {
	// not threadsafe; need to deep copy the playlist
	m := make(map[string]*m3u8.MasterPlaylist, 0)

	s.connectionLock.RLock()
	defer s.connectionLock.RUnlock()
	for _, cxn := range s.rtmpConnections {
		if cxn.sess == nil || cxn.pl == nil {
			continue
		}
		cpl := cxn.pl
		m[string(cpl.ManifestID())] = cpl.GetHLSMasterPlaylist()
	}
	return &net.NodeStatus{Manifests: m}
}

// Debug helpers
func (s *LivepeerServer) LatestPlaylist() core.PlaylistManager {
	s.connectionLock.RLock()
	defer s.connectionLock.RUnlock()
	cxn, ok := s.rtmpConnections[s.lastManifestID]
	if !ok || cxn.pl == nil {
		return nil
	}
	return cxn.pl
}

func shouldStopStream(err error) bool {
	return false
}
