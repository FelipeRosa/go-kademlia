package kad

import (
	"context"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/FelipeRosa/go-kademlia/gen/kad_pb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type NodeOption func(*host)

func WithLogger(logger *zap.Logger) NodeOption {
	return func(n *host) {
		n.logger = logger.With(
			zap.String("node_id", n.id.String()),
			zap.String("node_listen_address", n.listener.Addr().String()),
		)
	}
}

type PeerAddrInfo struct {
	Addresses []string
}

func PeerAddrInfoFromStrings(addresses ...string) PeerAddrInfo {
	var addrInfo PeerAddrInfo
	for _, address := range addresses {
		addrInfo.Addresses = append(addrInfo.Addresses, address)
	}

	return addrInfo
}

type PeerInfo struct {
	ID       ID
	AddrInfo PeerAddrInfo
}

type Host interface {
	ID() ID

	ListenAddr() net.Addr
	LocalAddrInfo() PeerAddrInfo

	Bootstrap(ctx context.Context, peerAddresses ...PeerAddrInfo) error
	Ping(ctx context.Context, address PeerAddrInfo) (bool, error)
	FindNode(ctx context.Context, nodeID ID) ([]PeerInfo, error)
	Store(ctx context.Context, key string, value []byte) error
	FindValue(ctx context.Context, key string) ([]byte, bool, error)

	KBuckets() *KBuckets

	Close() error
}

type host struct {
	logger *zap.Logger

	id       ID
	listener *PeerListener

	kBuckets *KBuckets

	valueStore *ValueStore
}

func NewHost(listenAddress string, options ...NodeOption) (Host, error) {
	listener, err := NewPeerListener(listenAddress)
	if err != nil {
		return nil, err
	}

	n := &host{
		logger:     zap.NewNop(),
		id:         NewID(),
		listener:   listener,
		valueStore: NewValueStore(),
	}
	n.kBuckets = NewKBuckets(n, 20, WithPingTimeout(time.Second))

	for _, opt := range options {
		opt(n)
	}

	go n.acceptPeersLoop()

	n.logger.Debug("started Kademlia host")
	return n, nil
}

func (n *host) ID() ID {
	return n.id
}

func (n *host) ListenAddr() net.Addr {
	return n.listener.Addr()
}

func (n *host) LocalAddrInfo() PeerAddrInfo {
	var addrInfo PeerAddrInfo

	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			inet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			if !(inet.IP.IsGlobalUnicast() || inet.IP.IsUnspecified()) {
				continue
			}

			addrString := net.JoinHostPort(inet.IP.String(), strconv.Itoa(n.listener.Port()))
			addrInfo.Addresses = append(addrInfo.Addresses, addrString)
		}
	}

	return addrInfo
}

func (n *host) Bootstrap(ctx context.Context, peerAddresses ...PeerAddrInfo) error {
	// ping peers to get their IDs
	var bootstrapSuccesses int
	for _, address := range peerAddresses {
		pong, _ := n.Ping(ctx, address)
		if pong {
			bootstrapSuccesses++
		}

		// stop if we exceeded the context's deadline
		select {
		case <-ctx.Done():
			return context.DeadlineExceeded
		default:
		}
	}

	if bootstrapSuccesses == 0 {
		return errors.New("none of the bootstrap peers responded")
	}

	// advertise our ID
	if _, err := n.FindNode(ctx, n.ID()); err != nil {
		return err
	}

	return nil
}

func (n *host) Ping(ctx context.Context, address PeerAddrInfo) (bool, error) {
	req := &kad_pb.Request{
		Header: &kad_pb.RequestHeader{
			RequesterId:             n.ID(),
			RequesterLocalAddresses: n.LocalAddrInfo().Addresses,
		},
		Body: &kad_pb.Request_Ping{
			Ping: &kad_pb.PingRequest{},
		},
	}

	res, err := n.peerRequest(ctx, address, req)
	if IsNetTimeoutError(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if res.GetPing() == nil {
		return false, errors.New("wrong response for PING")
	}

	return true, nil
}

type findNodeQueue struct {
	lock    sync.Mutex
	entries []PeerInfo
	dedup   map[string]struct{}
}

func (q *findNodeQueue) Enqueue(peers ...PeerInfo) {
	q.lock.Lock()
	defer q.lock.Unlock()

	var dedupedPeers []PeerInfo

	for _, peer := range peers {
		id := peer.ID.String()
		if _, found := q.dedup[id]; found {
			return
		}
		q.dedup[id] = struct{}{}

		dedupedPeers = append(dedupedPeers, peer)
	}

	q.entries = append(peers, q.entries...)
}

func (q *findNodeQueue) Dequeue() (PeerInfo, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.Len() == 0 {
		return PeerInfo{}, false
	}

	peerInfo := q.entries[0]
	q.entries = q.entries[1:]

	return peerInfo, true
}

func (q *findNodeQueue) Len() int {
	return len(q.entries)
}

func (n *host) FindNode(ctx context.Context, nodeID ID) ([]PeerInfo, error) {
	nodeQueue := &findNodeQueue{
		dedup: make(map[string]struct{}),
	}
	nodeQueue.Enqueue(n.kBuckets.KClosestTo(nodeID)...)

	closestPeerInfos := make([]PeerInfo, 0, 20)
	var closestPeerInfosLock sync.Mutex

	var foundNode *PeerInfo
	var foundNodeLock sync.RWMutex

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				foundNodeLock.RLock()
				if foundNode != nil {
					foundNodeLock.RUnlock()
					break
				}
				foundNodeLock.RUnlock()

				peer, ok := nodeQueue.Dequeue()
				if !ok {
					break
				}

				if peer.ID.Equal(nodeID) {
					foundNodeLock.Lock()
					foundNode = &peer
					foundNodeLock.Unlock()
				}

				req := &kad_pb.Request{
					Header: &kad_pb.RequestHeader{
						RequesterId:             n.ID(),
						RequesterLocalAddresses: n.LocalAddrInfo().Addresses,
					},
					Body: &kad_pb.Request_FindNode{
						FindNode: &kad_pb.FindNodeRequest{
							NodeId: nodeID,
						},
					},
				}
				res, err := n.peerRequest(ctx, peer.AddrInfo, req)
				if err != nil {
					n.logger.Warn(
						"failed requesting FIND_NODE",
						zap.String("peer_id", peer.ID.String()),
						zap.Strings("peer_addresses", peer.AddrInfo.Addresses),
						zap.Error(err),
					)
					continue
				}

				resBody := res.GetFindNode()
				if resBody == nil {
					n.logger.Warn("wrong response for FIND_NODE", zap.String("peer_id", peer.ID.String()))
					continue
				}

				var resPeers []PeerInfo
				for _, nodeInfo := range resBody.GetNodeInfos() {
					// let's not add ourselves to the peer queue
					if n.ID().Equal(nodeInfo.Id) {
						continue
					}

					resPeers = append(resPeers, PeerInfo{
						ID:       nodeInfo.Id,
						AddrInfo: PeerAddrInfo{Addresses: nodeInfo.Addresses},
					})
				}

				// here we add the new peers, sort them by distance to the ID we are looking for
				// and prune the peers that are the farthest
				closestPeerInfosLock.Lock()
				for _, peer := range resPeers {
					// let's not duplicate peers
					var alreadyIncluded bool
					for _, p := range closestPeerInfos {
						if p.ID.Equal(peer.ID) {
							alreadyIncluded = true
							break
						}
					}
					if alreadyIncluded {
						continue
					}

					closestPeerInfos = append(closestPeerInfos, peer)
				}

				sort.Slice(closestPeerInfos, func(i, j int) bool {
					return closestPeerInfos[i].ID.DistanceTo(nodeID) < closestPeerInfos[j].ID.DistanceTo(nodeID)
				})
				if len(closestPeerInfos) > 20 {
					closestPeerInfos = closestPeerInfos[:20]
				}
				closestPeerInfosLock.Unlock()

				// enqueue new closest peers
				nodeQueue.Enqueue(resPeers...)
			}
		}()
	}
	wg.Wait()

	if foundNode == nil {
		return closestPeerInfos, nil
	}
	return []PeerInfo{*foundNode}, nil

}

func (n *host) Store(ctx context.Context, key string, value []byte) error {
	id := IDFromString(key)

	closestNodes, err := n.FindNode(ctx, id)
	if err != nil {
		return err
	}

	req := &kad_pb.Request{
		Header: &kad_pb.RequestHeader{
			RequesterId:             n.ID(),
			RequesterLocalAddresses: n.LocalAddrInfo().Addresses,
		},
		Body: &kad_pb.Request_Store{
			Store: &kad_pb.StoreRequest{
				Key:   key,
				Value: value,
			},
		},
	}

	for _, node := range closestNodes {
		n.peerRequest(ctx, node.AddrInfo, req)
	}

	return nil
}

func (n *host) FindValue(ctx context.Context, key string) ([]byte, bool, error) {
	id := IDFromString(key)
	if val, found := n.valueStore.Get(id.String()); found {
		return val, true, nil
	}

	nodeQueue := &findNodeQueue{
		dedup: make(map[string]struct{}),
	}
	nodeQueue.Enqueue(n.kBuckets.KClosestTo(id)...)

	var value []byte
	var foundValue bool
	var foundValueLock sync.RWMutex

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				foundValueLock.RLock()
				if foundValue {
					foundValueLock.RUnlock()
					break
				}
				foundValueLock.RUnlock()

				peer, ok := nodeQueue.Dequeue()
				if !ok {
					break
				}

				req := &kad_pb.Request{
					Header: &kad_pb.RequestHeader{
						RequesterId:             n.ID(),
						RequesterLocalAddresses: n.LocalAddrInfo().Addresses,
					},
					Body: &kad_pb.Request_FindValue{
						FindValue: &kad_pb.FindValueRequest{
							Key: key,
						},
					},
				}
				res, err := n.peerRequest(ctx, peer.AddrInfo, req)
				if err != nil {
					n.logger.Warn(
						"failed requesting FIND_VALUE",
						zap.String("peer_id", peer.ID.String()),
						zap.Strings("peer_addresses", peer.AddrInfo.Addresses),
						zap.Error(err),
					)
					continue
				}

				resBody := res.GetFindValue()
				if resBody == nil {
					n.logger.Warn("wrong response for FIND_NODE", zap.String("peer_id", peer.ID.String()))
					continue
				}

				switch body := resBody.Val.(type) {
				case *kad_pb.FindValueResponse_NodesResponse:
					var resPeers []PeerInfo
					for _, nodeInfo := range body.NodesResponse.GetNodeInfos() {
						// let's not add ourselves to the peer queue
						if n.ID().Equal(nodeInfo.Id) {
							continue
						}

						resPeers = append(resPeers, PeerInfo{
							ID:       nodeInfo.Id,
							AddrInfo: PeerAddrInfo{Addresses: nodeInfo.Addresses},
						})
					}
					nodeQueue.Enqueue(resPeers...)

				case *kad_pb.FindValueResponse_ValueResponse:
					foundValueLock.Lock()
					foundValue = true
					value = body.ValueResponse.GetValue()
					foundValueLock.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	return value, foundValue, nil
}

func (n *host) KBuckets() *KBuckets {
	return n.kBuckets
}

func (n *host) Close() error {
	return n.listener.Close()
}

func (n *host) acceptPeersLoop() {
	for {
		peerConn, err := n.listener.Accept()
		if err != nil {
			if n.listener.Closed() {
				return
			}

			n.logger.Error("failed accepting peer connection", zap.Error(err))
			continue
		}
		go n.handlePeerConn(peerConn)
	}
}

func (n *host) handlePeerConn(peerConn *PeerConn) {
	defer peerConn.Close()

	for {
		peerReq := &kad_pb.Request{}
		err := peerConn.Receive(context.Background(), peerReq)
		if IsNetEOFError(err) {
			break
		} else if err != nil {
			n.logger.Error("failed accepting peer request", zap.Error(err))
			return
		}

		logger := n.logger.With(zap.String("peer_id", ID(peerReq.Header.RequesterId).String()))

		switch body := peerReq.GetBody().(type) {
		case *kad_pb.Request_Ping:
			logger = logger.With(zap.String("request_type", "PING"))
			logger.Debug("handling request")

			res := &kad_pb.Response{
				Header: &kad_pb.ResponseHeader{
					ResponderId:             n.ID(),
					ResponderLocalAddresses: n.LocalAddrInfo().Addresses,
				},
				Body: &kad_pb.Response_Ping{
					Ping: &kad_pb.PingResponse{},
				},
			}

			logger.Debug("sending response")
			sendCtx, cancelSendCtx := context.WithTimeout(context.Background(), time.Second)
			if err := peerConn.Send(sendCtx, res); err != nil {
				cancelSendCtx()
				logger.Error("failed sending response to peer", zap.Error(err))
				return
			}
			cancelSendCtx()

			logger.Debug("done handling request")

		case *kad_pb.Request_FindNode:
			logger = logger.With(zap.String("request_type", "FIND_NODE"))
			logger.Debug("handling request")

			var closestNodes []*kad_pb.NodeInfo
			for _, peer := range n.kBuckets.KClosestTo(body.FindNode.NodeId) {
				closestNodes = append(closestNodes, &kad_pb.NodeInfo{
					Id:        peer.ID,
					Addresses: peer.AddrInfo.Addresses,
				})
			}

			res := &kad_pb.Response{
				Header: &kad_pb.ResponseHeader{
					ResponderId:             n.ID(),
					ResponderLocalAddresses: n.LocalAddrInfo().Addresses,
				},
				Body: &kad_pb.Response_FindNode{
					FindNode: &kad_pb.FindNodeResponse{
						NodeInfos: closestNodes,
					},
				},
			}

			logger.Debug("sending response")
			sendCtx, cancelSendCtx := context.WithTimeout(context.Background(), time.Second)
			if err := peerConn.Send(sendCtx, res); err != nil {
				cancelSendCtx()
				logger.Error("failed sending response to peer", zap.Error(err))
				return
			}
			cancelSendCtx()

			logger.Debug("done handling request")

		case *kad_pb.Request_Store:
			logger = logger.With(zap.String("request_type", "STORE"))
			n.valueStore.Put(body.Store.GetKey(), body.Store.GetValue())

			res := &kad_pb.Response{
				Header: &kad_pb.ResponseHeader{
					ResponderId:             n.ID(),
					ResponderLocalAddresses: n.LocalAddrInfo().Addresses,
				},
				Body: &kad_pb.Response_Store{Store: &kad_pb.StoreResponse{}},
			}

			logger.Debug("sending response")
			sendCtx, cancelSendCtx := context.WithTimeout(context.Background(), time.Second)
			if err := peerConn.Send(sendCtx, res); err != nil {
				cancelSendCtx()
				logger.Error("failed sending response to peer", zap.Error(err))
				return
			}
			cancelSendCtx()

			logger.Debug("done handling request")

		case *kad_pb.Request_FindValue:
			logger = logger.With(zap.String("request_type", "STORE"))

			res := &kad_pb.Response{
				Header: &kad_pb.ResponseHeader{
					ResponderId:             n.ID(),
					ResponderLocalAddresses: n.LocalAddrInfo().Addresses,
				},
			}

			if value, found := n.valueStore.Get(body.FindValue.Key); found {
				res.Body = &kad_pb.Response_FindValue{
					FindValue: &kad_pb.FindValueResponse{
						Val: &kad_pb.FindValueResponse_ValueResponse{
							ValueResponse: &kad_pb.FindValueValueResponse{
								Value: value,
							},
						},
					},
				}
			} else {
				var closestNodes []*kad_pb.NodeInfo
				for _, peer := range n.kBuckets.KClosestTo(IDFromString(body.FindValue.Key)) {
					closestNodes = append(closestNodes, &kad_pb.NodeInfo{
						Id:        peer.ID,
						Addresses: peer.AddrInfo.Addresses,
					})
				}

				res.Body = &kad_pb.Response_FindValue{
					FindValue: &kad_pb.FindValueResponse{
						Val: &kad_pb.FindValueResponse_NodesResponse{
							NodesResponse: &kad_pb.FindValueNodesResponse{
								NodeInfos: closestNodes,
							},
						},
					},
				}
			}

			logger.Debug("sending response")
			sendCtx, cancelSendCtx := context.WithTimeout(context.Background(), time.Second)
			if err := peerConn.Send(sendCtx, res); err != nil {
				cancelSendCtx()
				logger.Error("failed sending response to peer", zap.Error(err))
				return
			}
			cancelSendCtx()

			logger.Debug("done handling request")

		default: // ignore unknown request types
			return
		}

		peerInfo := PeerInfo{
			ID: peerReq.Header.RequesterId,
			AddrInfo: PeerAddrInfoFromStrings(
				append(peerReq.Header.RequesterLocalAddresses, peerConn.RemoteAddr().String())...,
			),
		}
		if err := n.kBuckets.Insert(peerInfo); err != nil {
			logger.Error("failed inserting peer to k-buckets", zap.Error(err))
		}
	}
}

func (n *host) peerRequest(ctx context.Context, addrInfo PeerAddrInfo, req *kad_pb.Request) (*kad_pb.Response, error) {
	connCh := make(chan *PeerConn)

	var wg sync.WaitGroup
	connCtx, connCtxCancel := context.WithCancel(ctx)
	for _, address := range addrInfo.Addresses {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()

			peerConn, err := ConnectToPeer(connCtx, address)
			if err != nil {
				return
			}

			select {
			case connCh <- peerConn:
			default:
			}
		}(address)
	}
	// Close the channel if all goroutines finished with error
	go func() {
		wg.Wait()
		close(connCh)
	}()

	peerConn := <-connCh
	connCtxCancel()
	if peerConn == nil {
		return nil, errors.New("failed connecting to peer")
	}
	defer peerConn.Close()

	logger := n.logger.With(zap.String("peer_address", peerConn.RemoteAddr().String()))

	logger.Debug("sending peer request")
	if err := peerConn.Send(ctx, req); err != nil {
		return nil, err
	}

	logger.Debug("waiting peer response")
	res := &kad_pb.Response{}
	if err := peerConn.Receive(ctx, res); err != nil {
		return nil, err
	}
	logger.Debug(
		"received peer response",
		zap.String("peer_id", ID(res.Header.ResponderId).String()),
	)

	peerInfo := PeerInfo{
		ID: res.Header.ResponderId,
		AddrInfo: PeerAddrInfoFromStrings(
			append(res.Header.ResponderLocalAddresses, peerConn.RemoteAddr().String())...,
		),
	}
	if err := n.kBuckets.Insert(peerInfo); err != nil {
		logger.Error("failed inserting peer to k-buckets", zap.Error(err))
	}
	return res, nil
}
