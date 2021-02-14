package kad

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

func IsNetTimeoutError(err error) bool {
	err = errors.Cause(err)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}

func IsNetEOFError(err error) bool {
	return errors.Cause(err) == io.EOF
}

type PeerConn struct {
	conn net.Conn
}

func ConnectToPeer(ctx context.Context, addr string) (*PeerConn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	return &PeerConn{conn: conn}, nil
}

func (pc *PeerConn) LocalAddr() net.Addr {
	return pc.conn.LocalAddr()
}

func (pc *PeerConn) RemoteAddr() net.Addr {
	return pc.conn.RemoteAddr()
}

func (pc *PeerConn) Send(ctx context.Context, message proto.Message) error {
	if deadline, exists := ctx.Deadline(); exists {
		if err := pc.conn.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "setting connection write deadline")
		}
	}

	msgBytes, err := proto.Marshal(message)
	if err != nil {
		return errors.Wrap(err, "marshalling message")
	}

	if err := binary.Write(pc.conn, binary.BigEndian, uint32(len(msgBytes))); err != nil {
		return errors.Wrap(err, "writing message length")
	}

	if _, err := pc.conn.Write(msgBytes); err != nil {
		return errors.Wrap(err, "writing message bytes")
	}

	return nil
}

func (pc *PeerConn) Receive(ctx context.Context, message proto.Message) error {
	if deadline, exists := ctx.Deadline(); exists {
		if err := pc.conn.SetReadDeadline(deadline); err != nil {
			return errors.Wrap(err, "setting connection read deadline")
		}
	}

	var msgLength uint32
	if err := binary.Read(pc.conn, binary.BigEndian, &msgLength); err != nil {
		return errors.Wrap(err, "reading message length")
	}

	buf := make([]byte, msgLength)
	if _, err := pc.conn.Read(buf); err != nil {
		return errors.Wrap(err, "reading message bytes")
	}

	if err := proto.Unmarshal(buf, message); err != nil {
		return errors.Wrap(err, "unmarshalling protobuf message")
	}

	return nil
}

func (pc *PeerConn) Close() error {
	return pc.conn.Close()
}

type PeerListener struct {
	listener net.Listener
	port     int
	closed   bool

	lock sync.RWMutex
}

func NewPeerListener(listenAddress string) (*PeerListener, error) {
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return nil, err
	}

	return &PeerListener{listener: listener, port: listener.Addr().(*net.TCPAddr).Port}, nil
}

func (pl *PeerListener) Addr() net.Addr {
	return pl.listener.Addr()
}

func (pl *PeerListener) Port() int {
	return pl.port
}

func (pl *PeerListener) Accept() (*PeerConn, error) {
	conn, err := pl.listener.Accept()
	if err != nil {
		return nil, err
	}

	return &PeerConn{conn: conn}, nil
}

func (pl *PeerListener) Close() error {
	pl.lock.Lock()
	defer pl.lock.Unlock()
	pl.closed = true

	return pl.listener.Close()
}

func (pl *PeerListener) Closed() bool {
	pl.lock.RLock()
	defer pl.lock.RUnlock()
	return pl.closed
}
