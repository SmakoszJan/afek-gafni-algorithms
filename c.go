//go:build ignore

package main

import (
	"context"
	"fmt"
	"math/bits"
	"math/rand"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	BASIC                     = 0
	DELAY_LEVEL               = 1
	BOUNCE_INSUFFICIENT_LEVEL = 2
)

type Message struct {
	Level      int
	SenderID   int
	SourceLink int
	/// When sent by candidate, implies this capture may be redirected to parent
	/// Whent sent by ordinary, implies the candidate has to send a delegate
	Delegate bool
}

type NodeState struct {
	ID                 int
	Level              int
	OwnerID            int
	PotentialFather    int
	HasReceivedMessage bool
	IsCandidate        bool
	Mu                 sync.Mutex
}

type OrdinaryProcess struct {
	State             *NodeState
	Inbox             chan Message
	CandidateOutboxes map[int]chan Message
	Ctx               context.Context
	TotalMessages     *int64
}

func (o *OrdinaryProcess) RunOrdinary(log bool, variant int) {
	for {
		select {
		case <-o.Ctx.Done():
			return
		case msg, ok := <-o.Inbox:
			if !ok {
				return
			}
			o.processMessage(msg, log, variant)
		}
	}
}

func (o *OrdinaryProcess) processMessage(msg Message, log bool, variant int) {
	o.State.Mu.Lock()
	defer o.State.Mu.Unlock()

	o.State.HasReceivedMessage = true
	senderID := msg.SenderID
	msg.SourceLink = o.State.ID

	if variant == BOUNCE_INSUFFICIENT_LEVEL && !msg.Delegate && msg.Level <= o.State.Level {
		msg.Delegate = true

		if log {
			fmt.Printf("[ZWYKŁY %d] Mniejszy poziom. Odbija wiadomość od Kandydata %d\n", o.State.ID, senderID)
		}

		if candChan, ok := o.CandidateOutboxes[senderID]; ok {
			candChan <- msg
			atomic.AddInt64(o.TotalMessages, 1)
		}
		return
	}

	if msg.Level < o.State.Level {
		if log {
			fmt.Printf("[ZWYKŁY %d] Ignoruje wiadomość od Kandydata %d: level wiadomości (%d) < level węzła (%d)\n", o.State.ID, senderID, msg.Level, o.State.Level)
		}
		return
	}

	if msg.Level > o.State.Level {
		o.State.Level = msg.Level
		o.State.OwnerID = senderID
		o.State.PotentialFather = -1

		if log {
			fmt.Printf("[ZWYKŁY %d] Bezkonfliktowe przejęcie przez Kandydata %d\n", o.State.ID, senderID)
		}

		msg.Delegate = false
		if candChan, ok := o.CandidateOutboxes[senderID]; ok {
			candChan <- msg
			atomic.AddInt64(o.TotalMessages, 1)
		}
		return
	}

	if msg.Level == o.State.Level {
		if senderID < o.State.OwnerID {
			if log {
				fmt.Printf("[ZWYKŁY %d] Ignoruje wiadomość od Kandydata %d: level równy (%d), ale senderID (%d) < ownerID (%d)\n", o.State.ID, senderID, o.State.Level, senderID, o.State.OwnerID)
			}
			return
		}

		if senderID == o.State.PotentialFather {
			msg.Level++
			o.State.OwnerID = senderID
			o.State.PotentialFather = -1

			if log {
				fmt.Printf("[ZWYKŁY %d] Wrócił potential father - kandydat %d\n", o.State.ID, senderID)
			}

			msg.SourceLink = o.State.ID
			msg.Delegate = false
			if candChan, ok := o.CandidateOutboxes[senderID]; ok {
				candChan <- msg
				atomic.AddInt64(o.TotalMessages, 1)
			}
			return
		}

		if o.State.PotentialFather != -1 {
			if log {
				fmt.Printf("[ZWYKŁY %d] Ignoruje wiadomość od Kandydata %d: potential_father jest już zajęty\n", o.State.ID, senderID)
			}
			return
		}

		if msg.Delegate {
			ownerID := o.State.OwnerID
			o.State.PotentialFather = senderID

			if log {
				fmt.Printf("[ZWYKŁY %d] Przekierowuje wiadomość od Kandydata %d i ustawia potential_father\n", o.State.ID, senderID)
			}

			if candChan, ok := o.CandidateOutboxes[ownerID]; ok {
				candChan <- msg
				atomic.AddInt64(o.TotalMessages, 1)
			}
		} else {
			msg.Delegate = true

			if log {
				fmt.Printf("[ZWYKŁY %d] Równe poziomy. Odbija wiadomość od Kandydata %d\n", o.State.ID, senderID)
			}

			if candChan, ok := o.CandidateOutboxes[senderID]; ok {
				candChan <- msg
				atomic.AddInt64(o.TotalMessages, 1)
			}
		}

	}
}

func int_ceil_log2(v uint64) int {
	if v <= 1 {
		return 0
	}

	return bits.Len64(v - 1)
}

func int_floor_log2(v uint64) int {
	return bits.Len64(v) - 1
}

type CandidateProcess struct {
	ID                  int
	Size                int
	Level               int
	Killed              bool
	Untraversed         []int
	Inbox               chan Message
	OrdinaryMap         map[int]chan Message
	Wg                  *sync.WaitGroup
	Ctx                 context.Context
	Cancel              context.CancelFunc
	TotalMessages       *int64
	LeadersElected      *int32
	TotalNetworkSize    int
	TotalNetworkSizeLog int
}

func (c *CandidateProcess) RunCandidate(delay time.Duration, log bool, variant int) {
	defer c.Wg.Done()

	failedCapture := -1
	for len(c.Untraversed) > 0 {
		pendingCaptures := 1

		if failedCapture == -1 {
			if c.Level > c.TotalNetworkSizeLog {
				pendingCaptures = min(c.TotalNetworkSize/c.TotalNetworkSizeLog, len(c.Untraversed))
			}

			// W zasadzie niepotrzebne, można też brać prefix, ale na potrzeby prezentacji zostawiamy
			rand.Shuffle(len(c.Untraversed), func(i, j int) {
				c.Untraversed[i], c.Untraversed[j] = c.Untraversed[j], c.Untraversed[i]
			})

			for _, l := range c.Untraversed[:pendingCaptures] {
				if log {
					var weak string
					if pendingCaptures == 1 {
						weak = "silną"
					} else {
						weak = "słabą"
					}

					fmt.Printf("[KANDYDAT %d] Wysyła %s wiadomość (poziom %d) do węzła zwykłego %d\n", c.ID, weak, c.Level, l)
				}
				if targetChan, ok := c.OrdinaryMap[l]; ok {
					targetChan <- Message{Level: c.Level, SenderID: c.ID, SourceLink: l, Delegate: pendingCaptures == 1}
					atomic.AddInt64(c.TotalMessages, 1)
				}
			}
		} else {
			l := failedCapture
			failedCapture = -1
			if log {
				fmt.Printf("[KANDYDAT %d] Wysyła silną wiadomość (poziom %d) do węzła zwykłego %d\n", c.ID, c.Level, l)
			}
			if targetChan, ok := c.OrdinaryMap[l]; ok {
				targetChan <- Message{
					Level:      c.Level,
					SenderID:   c.ID,
					SourceLink: l,
					Delegate:   true,
				}
				atomic.AddInt64(c.TotalMessages, 1)
			}
		}

		for pendingCaptures > 0 {
			select {
			case <-c.Ctx.Done():
				return
			case msg, ok := <-c.Inbox:
				if !ok {
					return
				}

				if msg.SenderID == c.ID && !c.Killed {
					pendingCaptures--
					if msg.Delegate {
						failedCapture = msg.SourceLink
						continue
					}

					c.Size++
					c.Level = msg.Level
					if variant != DELAY_LEVEL {
						past := c.Level
						c.Level = max(c.Level, int_ceil_log2(uint64(c.Size)))
						if past < c.Level && log {
							fmt.Printf("[KANDTDAT %d] Zwiększa poziom do %d.\n", c.ID, c.Level)
						}
					}

					idx := -1
					for i, l := range c.Untraversed {
						if l == msg.SourceLink {
							idx = i
							break
						}
					}

					c.Untraversed = append(c.Untraversed[:idx], c.Untraversed[idx+1:]...)
					continue
				}

				if (msg.Level < c.Level) || (msg.Level == c.Level && msg.SenderID < c.ID) || c.Killed {
					if log {
						fmt.Printf("[KANDYDAT %d] Ignoruje wiadomość Kandydata %d\n", c.ID, msg.SenderID)
					}
					continue
				}

				c.Killed = true
				if log {
					fmt.Printf("[ŚMIERĆ KANDYDATA] Kandydat %d (poziom: %d) został pokonany przez Kandydata %d\n", c.ID, c.Level, msg.SenderID)
				}

				if targetChan, ok := c.OrdinaryMap[msg.SourceLink]; ok {
					targetChan <- msg
					atomic.AddInt64(c.TotalMessages, 1)
				}
			}
		}

		if !c.Killed && variant == DELAY_LEVEL {
			past := c.Level
			c.Level = max(c.Level, int_ceil_log2(uint64(c.Size)))
			if past < c.Level && log {
				fmt.Printf("[KANDTDAT %d] Zwiększa poziom do %d.\n", c.ID, c.Level)
			}
		}
	}

	time.Sleep(delay)
	if !c.Killed {
		if log {
			fmt.Printf("[LIDER WYBRANY] Kandydat %d\n", c.ID)
		}
		atomic.AddInt32(c.LeadersElected, 1)
		c.Cancel()
	} else if log {
		fmt.Printf("[KANDYDAT %d] Zakończył działanie jako wyeliminowany.\n", c.ID)
	}
}

func runSimulation(n int, log bool, variant int) int64 {
	var totalMessages int64 = 0
	var leadersElected int32 = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ordinaryChannels := make(map[int]chan Message, n)
	candidateChannels := make(map[int]chan Message)

	bufferSize := n * 10
	for i := range n {
		ordinaryChannels[i] = make(chan Message, bufferSize)
		candidateChannels[i] = make(chan Message, bufferSize)
	}

	nodeStates := make([]*NodeState, n)
	var ordinaryWg sync.WaitGroup

	for i := range n {
		nodeStates[i] = &NodeState{
			ID:                 i,
			Level:              -1,
			OwnerID:            -1,
			PotentialFather:    -1,
			HasReceivedMessage: false,
			IsCandidate:        false,
		}

		op := &OrdinaryProcess{
			State:             nodeStates[i],
			Inbox:             ordinaryChannels[i],
			CandidateOutboxes: candidateChannels,
			Ctx:               ctx,
			TotalMessages:     &totalMessages,
		}

		ordinaryWg.Add(1)
		go func(p *OrdinaryProcess) {
			defer ordinaryWg.Done()
			p.RunOrdinary(log, variant)
		}(op)
	}

	var candidateWg sync.WaitGroup

	for i := range n {
		nodeID := i
		candidateWg.Add(1)

		go func(id int) {
			delay := time.Duration(rand.Intn(50)) * time.Millisecond
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				candidateWg.Done()
				return
			}

			nodeStates[id].Mu.Lock()
			if nodeStates[id].HasReceivedMessage || nodeStates[id].IsCandidate {
				nodeStates[id].Mu.Unlock()
				candidateWg.Done()
				return
			}
			nodeStates[id].IsCandidate = true
			nodeStates[id].HasReceivedMessage = true
			nodeStates[id].Mu.Unlock()

			c := &CandidateProcess{
				ID:                  id,
				Size:                0,
				Level:               0,
				Killed:              false,
				Untraversed:         getOtherNodes(n),
				Inbox:               candidateChannels[id],
				OrdinaryMap:         ordinaryChannels,
				Wg:                  &candidateWg,
				Ctx:                 ctx,
				Cancel:              cancel,
				TotalMessages:       &totalMessages,
				LeadersElected:      &leadersElected,
				TotalNetworkSize:    n,
				TotalNetworkSizeLog: int_floor_log2(uint64(n)),
			}

			c.RunCandidate(20*time.Millisecond, log, variant)
		}(nodeID)
	}

	candidateWg.Wait()

	if atomic.LoadInt32(&leadersElected) != 1 {
		fmt.Printf("Liczba wybranych liderów dla n=%d wynosi: %d\n", n, atomic.LoadInt32(&leadersElected))
	}

	cancel()

	for i := range n {
		close(ordinaryChannels[i])
	}

	ordinaryWg.Wait()

	return atomic.LoadInt64(&totalMessages)
}

func getOtherNodes(n int) []int {
	var list []int
	for i := range n {
		list = append(list, i)
	}
	return list
}

func calculateStats(results []int64) (float64, float64) {
	if len(results) == 0 {
		return 0, 0
	}

	sorted := make([]int64, len(results))
	copy(sorted, results)
	slices.Sort(sorted)

	var sum int64 = 0
	for _, val := range sorted {
		sum += val
	}
	mean := float64(sum) / float64(len(sorted))

	var median float64
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		median = float64(sorted[mid-1]+sorted[mid]) / 2.0
	} else {
		median = float64(sorted[mid])
	}

	return mean, median
}

func bench() {
	nValues := []int{10, 100, 1000, 2500}
	numRuns := 5

	for _, n := range nValues {
		fmt.Printf("n = %d\n", n)
		results := make([]int64, numRuns)

		for run := range numRuns {
			msgCount := runSimulation(n, false, BOUNCE_INSUFFICIENT_LEVEL)
			results[run] = msgCount
		}

		mean, median := calculateStats(results)
		fmt.Printf("Average Messages: %.2f\n", mean)
		fmt.Printf("Median Messages:  %.2f\n\n", median)
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--bench" {
		bench()
		return
	}

	const N = 50
	const LOG = true
	// BASIC/DELAY_LEVEL/BOUNCE_INSUFFICIENT_LEVEL
	const VARIANT = BASIC

	runSimulation(N, LOG, VARIANT)
}
