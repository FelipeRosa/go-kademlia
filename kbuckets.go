package kad

import (
	"bytes"
	"context"
	"sync"
	"time"
)

type KBucketsOption func(entry *KBuckets)

func WithPingTimeout(timeout time.Duration) KBucketsOption {
	return func(entry *KBuckets) {
		entry.pingTimeout = timeout
	}
}

type KBuckets struct {
	k           int
	node        Host
	pingTimeout time.Duration
	entries     [][]PeerInfo

	lock sync.RWMutex
}

func NewKBuckets(node Host, k int, opts ...KBucketsOption) *KBuckets {
	kb := &KBuckets{
		k:           k,
		node:        node,
		pingTimeout: time.Second,
		entries:     make([][]PeerInfo, IDLength*8),
	}
	for _, opt := range opts {
		opt(kb)
	}

	return kb
}

func (kb *KBuckets) Insert(peer PeerInfo) error {
	if bytes.Equal(peer.ID, kb.node.ID()) {
		return nil
	}

	d := peer.ID.DistanceTo(kb.node.ID())

	kb.lock.RLock()
	foundAt := -1
	for i, peerInfo := range kb.entries[d] {
		if peerInfo.ID.Equal(peer.ID) {
			foundAt = i
			break
		}
	}
	kb.lock.RUnlock()

	// remove peer from bucket so we'll move it to the end of the bucket
	if foundAt != -1 {
		kb.lock.Lock()
		kb.entries[d] = append(kb.entries[d][0:foundAt], kb.entries[d][foundAt+1:len(kb.entries[d])]...)
		kb.lock.Unlock()
	}

	if len(kb.entries[d]) < kb.k {
		kb.lock.Lock()
		kb.entries[d] = append(kb.entries[d], peer)
		kb.lock.Unlock()
		return nil
	}

	leastSeenPeer := kb.entries[d][0]
	ctx, cancel := context.WithTimeout(context.Background(), kb.pingTimeout)
	defer cancel()
	pong, err := kb.node.Ping(ctx, leastSeenPeer.AddrInfo)
	if err != nil {
		return err
	}

	if pong {
		return nil
	}

	kb.lock.Lock()
	kb.entries[d] = append(kb.entries[d][1:], peer)
	kb.lock.Unlock()

	return nil
}

func (kb *KBuckets) KClosestTo(id ID) []PeerInfo {
	kb.lock.RLock()
	defer kb.lock.RUnlock()

	d := id.DistanceTo(kb.node.ID())

	var kClosest []PeerInfo
	for up, down := d, d-1; ; {
		if up < 160 {
			for _, entry := range kb.entries[up] {
				kClosest = append(kClosest, entry)
			}
			up++
		}

		if down >= 0 {
			for _, entry := range kb.entries[down] {
				kClosest = append(kClosest, entry)
			}
			down--
		}

		if up >= 159 && down <= 0 {
			break
		}
	}

	return kClosest
}

func (kb *KBuckets) Entries() [][]PeerInfo {
	return kb.entries
}
