//go:build ignore

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Message struct {
	Level      int
	SenderID   int
	SourceLink int
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

func (o *OrdinaryProcess) RunOrdinary(log bool) {
	for {
		select {
		case <-o.Ctx.Done():
			return
		case msg, ok := <-o.Inbox:
			if !ok {
				return
			}
			o.processMessage(msg, log)
		}
	}
}

func (o *OrdinaryProcess) processMessage(msg Message, log bool) {
	o.State.Mu.Lock()
	defer o.State.Mu.Unlock()

	o.State.HasReceivedMessage = true
	senderID := msg.SenderID

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

		msg.SourceLink = o.State.ID
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

		o.State.PotentialFather = senderID
		msg.SourceLink = o.State.ID
		ownerID := o.State.OwnerID

		if log {
			fmt.Printf("[ZWYKŁY %d] Przekierowuje wiadomość od Kandydata %d i ustawia potential_father\n", o.State.ID, senderID)
		}

		if candChan, ok := o.CandidateOutboxes[ownerID]; ok {
			candChan <- msg
			atomic.AddInt64(o.TotalMessages, 1)
		}
	}
}

type CandidateProcess struct {
	ID             int
	Level          int
	Killed         bool
	Untraversed    []int
	Inbox          chan Message
	OrdinaryMap    map[int]chan Message
	Wg             *sync.WaitGroup
	Ctx            context.Context
	Cancel         context.CancelFunc
	TotalMessages  *int64
	LeadersElected *int32
}

func (c *CandidateProcess) RunCandidate(delay time.Duration, log bool) {
	defer c.Wg.Done()

	for len(c.Untraversed) > 0 {
		idx := rand.Intn(len(c.Untraversed))
		l := c.Untraversed[idx]

		if log {
			fmt.Printf("[KANDYDAT %d] Wysyła wiadomość (poziom %d) do węzła zwykłego %d\n", c.ID, c.Level, l)
		}
		if targetChan, ok := c.OrdinaryMap[l]; ok {
			targetChan <- Message{
				Level:      c.Level,
				SenderID:   c.ID,
				SourceLink: l,
			}
			atomic.AddInt64(c.TotalMessages, 1)
		}

		waitingForOurReturn := true
		for waitingForOurReturn {
			select {
			case <-c.Ctx.Done():
				return
			case msg, ok := <-c.Inbox:
				if !ok {
					return
				}

				if msg.SenderID == c.ID && !c.Killed {
					c.Level = msg.Level
					waitingForOurReturn = false

					c.Untraversed = append(c.Untraversed[:idx], c.Untraversed[idx+1:]...)
					break
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

func runSimulation(n int, log bool) int64 {
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
			p.RunOrdinary(log)
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
				ID:             id,
				Level:          0,
				Killed:         false,
				Untraversed:    getOtherNodes(n),
				Inbox:          candidateChannels[id],
				OrdinaryMap:    ordinaryChannels,
				Wg:             &candidateWg,
				Ctx:            ctx,
				Cancel:         cancel,
				TotalMessages:  &totalMessages,
				LeadersElected: &leadersElected,
			}

			c.RunCandidate(delay, log)
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
			msgCount := runSimulation(n, false)
			results[run] = msgCount
		}

		mean, median := calculateStats(results)
		fmt.Printf("Average Messages: %.2f\n", mean)
		fmt.Printf("Median Messages:  %.2f\n\n", median)
	}
}

func getOtherNodes(n int) []int {
	var list []int
	for i := range n {
		list = append(list, i)
	}
	return list
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--bench" {
		bench()
		return
	}

	const N = 50
	const LOG = true

	runSimulation(N, LOG)
}
