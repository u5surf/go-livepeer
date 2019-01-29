package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"

	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"
	"github.com/livepeer/go-livepeer/pm"
	ffmpeg "github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/stream"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const GRPCTimeout = 8 * time.Second
const PaymentHeader = "Livepeer-Payment"
const HTTPTimeout = 8 * time.Second

var tlsConfig = &tls.Config{InsecureSkipVerify: true}
var httpClient = &http.Client{
	Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	Timeout:   HTTPTimeout,
}

type Orchestrator interface {
	ServiceURI() *url.URL
	Address() ethcommon.Address
	TranscoderSecret() string
	Sign([]byte) ([]byte, error)
	VerifySig(ethcommon.Address, string, []byte) bool
	CurrentBlock() *big.Int
	TranscodeSeg(*core.SegTranscodingMetadata, *stream.HLSSegment) (*core.TranscodeResult, error)
	ServeTranscoder(stream net.Transcoder_RegisterTranscoderServer)
	TranscoderResults(job int64, res *core.RemoteTranscoderResult)
	ProcessPayment(payment net.Payment, manifestID core.ManifestID) error
	TicketParams(sender ethcommon.Address) *net.TicketParams
}

type Broadcaster interface {
	Address() ethcommon.Address
	Sign([]byte) ([]byte, error)
}

// Session-specific state for broadcasters
type BroadcastSession struct {
	Broadcaster      Broadcaster
	ManifestID       core.ManifestID
	Profiles         []ffmpeg.VideoProfile
	OrchestratorInfo *net.OrchestratorInfo
	OrchestratorOS   drivers.OSSession
	BroadcasterOS    drivers.OSSession
	Sender           pm.Sender
	PMSessionID      string
}

// XXX do something about the implicit start of the http mux? this smells
func StartTranscodeServer(orch Orchestrator, bind string, mux *http.ServeMux, workDir string) {
	s := grpc.NewServer()
	lp := lphttp{
		orchestrator: orch,
		orchRpc:      s,
		transRpc:     mux,
	}
	net.RegisterOrchestratorServer(s, &lp)
	net.RegisterTranscoderServer(s, &lp)
	lp.transRpc.HandleFunc("/segment", lp.serveSegment)
	lp.transRpc.HandleFunc("/transcodeResults", lp.TranscodeResults)

	cert, key, err := getCert(orch.ServiceURI(), workDir)
	if err != nil {
		return // XXX return error
	}

	glog.Info("Listening for RPC on ", bind)
	srv := http.Server{
		Addr:    bind,
		Handler: &lp,
		// XXX doesn't handle streaming RPC well; split remote transcoder RPC?
		//ReadTimeout:  HTTPTimeout,
		//WriteTimeout: HTTPTimeout,
	}
	srv.ListenAndServeTLS(cert, key)
}

func CheckOrchestratorAvailability(orch Orchestrator) bool {
	ts := time.Now()
	ts_signature, err := orch.Sign([]byte(fmt.Sprintf("%v", ts)))
	if err != nil {
		return false
	}

	ping := crypto.Keccak256(ts_signature)

	orch_client, conn, err := startOrchestratorClient(orch.ServiceURI())
	if err != nil {
		return false
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), GRPCTimeout)
	defer cancel()

	pong, err := orch_client.Ping(ctx, &net.PingPong{Value: ping})
	if err != nil {
		glog.Error("Was not able to submit Ping: ", err)
		return false
	}

	return orch.VerifySig(orch.Address(), string(ping), pong.Value)
}

func GetOrchestratorInfo(ctx context.Context, bcast Broadcaster, orchestratorServer *url.URL) (*net.OrchestratorInfo, error) {
	c, conn, err := startOrchestratorClient(orchestratorServer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req, err := genOrchestratorReq(bcast)
	r, err := c.GetOrchestrator(ctx, req)
	if err != nil {
		glog.Errorf("Could not get orchestrator %v: %v", orchestratorServer, err)
		return nil, errors.New("Could not get orchestrator: " + err.Error())
	}

	return r, nil
}

func SubmitSegment(sess *BroadcastSession, seg *stream.HLSSegment, nonce uint64) (*net.TranscodeData, error) {
	if monitor.Enabled {
		monitor.SegmentUploadStart(nonce, seg.SeqNo)
	}
	uploaded := seg.Name != "" // hijack seg.Name to convey the uploaded URI

	segCreds, err := genSegCreds(sess, seg)
	if err != nil {
		if monitor.Enabled {
			monitor.LogSegmentUploadFailed(nonce, seg.SeqNo, err.Error())
		}
		return nil, err
	}
	data := seg.Data
	if uploaded {
		data = []byte(seg.Name)
	}

	payment, err := genPayment(sess)
	if err != nil {
		glog.Errorf("Could not create payment: %v", err)
	}

	ti := sess.OrchestratorInfo
	req, err := http.NewRequest("POST", ti.Transcoder+"/segment", bytes.NewBuffer(data))
	if err != nil {
		glog.Error("Could not generate trascode request to ", ti.Transcoder)
		if monitor.Enabled {
			monitor.LogSegmentUploadFailed(nonce, seg.SeqNo, err.Error())
		}
		return nil, err
	}

	req.Header.Set("Livepeer-Segment", segCreds)
	req.Header.Set("Livepeer-Payment", payment)
	if uploaded {
		req.Header.Set("Content-Type", "application/vnd+livepeer.uri")
	} else {
		req.Header.Set("Content-Type", "video/MP2T")
	}

	glog.Infof("Submitting segment %v : %v bytes", seg.SeqNo, len(data))
	start := time.Now()
	resp, err := httpClient.Do(req)
	uploadDur := time.Since(start)
	if err != nil {
		glog.Error("Unable to submit segment ", seg.SeqNo, err)
		if monitor.Enabled {
			monitor.LogSegmentUploadFailed(nonce, seg.SeqNo, err.Error())
		}
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		data, _ := ioutil.ReadAll(resp.Body)
		errorString := strings.TrimSpace(string(data))
		glog.Errorf("Error submitting segment %d: code %d error %v", seg.SeqNo, resp.StatusCode, string(data))
		if monitor.Enabled {
			monitor.LogSegmentUploadFailed(nonce, seg.SeqNo,
				fmt.Sprintf("Code: %d Error: %s", resp.StatusCode, errorString))
		}
		return nil, fmt.Errorf(errorString)
	}
	glog.Infof("Uploaded segment %v", seg.SeqNo)
	if monitor.Enabled {
		monitor.LogSegmentUploaded(nonce, seg.SeqNo, uploadDur)
	}

	data, err = ioutil.ReadAll(resp.Body)
	tookAllDur := time.Since(start)

	if err != nil {
		glog.Error(fmt.Sprintf("Unable to read response body for segment %v : %v", seg.SeqNo, err))
		if monitor.Enabled {
			monitor.LogSegmentTranscodeFailed("ReadBody", nonce, seg.SeqNo, err)
		}
		return nil, err
	}
	transcodeDur := tookAllDur - uploadDur

	var tr net.TranscodeResult
	err = proto.Unmarshal(data, &tr)
	if err != nil {
		glog.Error(fmt.Sprintf("Unable to parse response for segment %v : %v", seg.SeqNo, err))
		if monitor.Enabled {
			monitor.LogSegmentTranscodeFailed("ParseResponse", nonce, seg.SeqNo, err)
		}
		return nil, err
	}

	// check for errors and exit early if there's anything unusual
	var tdata *net.TranscodeData
	switch res := tr.Result.(type) {
	case *net.TranscodeResult_Error:
		err = fmt.Errorf(res.Error)
		glog.Errorf("Transcode failed for segment %v: %v", seg.SeqNo, err)
		if err.Error() == "MediaStats Failure" {
			glog.Info("Ensure the keyframe interval is 4 seconds or less")
		}
		if monitor.Enabled {
			monitor.LogSegmentTranscodeFailed("Transcode", nonce, seg.SeqNo, err)
		}
		return nil, err
	case *net.TranscodeResult_Data:
		// fall through here for the normal case
		tdata = res.Data
	default:
		glog.Error("Unexpected or unset transcode response field for ", seg.SeqNo)
		err = fmt.Errorf("UnknownResponse")
		if monitor.Enabled {
			monitor.LogSegmentTranscodeFailed("UnknownResponse", nonce, seg.SeqNo, err)
		}
		return nil, err
	}

	// transcode succeeded; continue processing response
	if monitor.Enabled {
		monitor.LogSegmentTranscoded(nonce, seg.SeqNo, transcodeDur, tookAllDur)
	}

	glog.Info("Successfully transcoded segment ", seg.SeqNo)

	return tdata, nil
}
