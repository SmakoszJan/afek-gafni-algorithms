//go:build ignore

package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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

func (o *OrdinaryProcess) RunOrdinary() {
	for {
		select {
		case <-o.Ctx.Done():
			return
		case msg, ok := <-o.Inbox:
			if !ok {
				return
			}
			o.processMessage(msg)
		}
	}
}

func (o *OrdinaryProcess) processMessage(msg Message) {
	o.State.Mu.Lock()
	defer o.State.Mu.Unlock()

	o.State.HasReceivedMessage = true
	senderID := msg.SenderID

	if msg.Level < o.State.Level {
		fmt.Printf("[ZWYKŁY %d] Ignoruje wiadomość od Kandydata %d: level wiadomości (%d) < level węzła (%d)\n", o.State.ID, senderID, msg.Level, o.State.Level)
		return
	}

	if msg.Level > o.State.Level {
		o.State.Level = msg.Level
		o.State.OwnerID = senderID
		o.State.PotentialFather = -1

		msg.SourceLink = o.State.ID
		if candChan, ok := o.CandidateOutboxes[senderID]; ok {
			candChan <- msg
			atomic.AddInt64(o.TotalMessages, 1)
		}
		return
	}

	if msg.Level == o.State.Level {
		if senderID < o.State.OwnerID {
			fmt.Printf("[ZWYKŁY %d] Ignoruje wiadomość od Kandydata %d: level równy (%d), ale senderID (%d) < ownerID (%d)\n", o.State.ID, senderID, o.State.Level, senderID, o.State.OwnerID)
			return
		}

		if senderID == o.State.PotentialFather {
			msg.Level++
			o.State.OwnerID = senderID
			o.State.PotentialFather = -1

			msg.SourceLink = o.State.ID
			if candChan, ok := o.CandidateOutboxes[senderID]; ok {
				candChan <- msg
				atomic.AddInt64(o.TotalMessages, 1)
			}
			return
		}

		if o.State.PotentialFather != -1 {
			fmt.Printf("[ZWYKŁY %d] Ignoruje wiadomość od Kandydata %d: potential_father jest już zajęty\n", o.State.ID, senderID)
			return
		}

		o.State.PotentialFather = senderID
		msg.SourceLink = o.State.ID
		ownerID := o.State.OwnerID

		if candChan, ok := o.CandidateOutboxes[ownerID]; ok {
			candChan <- msg
			atomic.AddInt64(o.TotalMessages, 1)
		}
	}
}

type CandidateProcess struct {
	ID                int
	Level             int
	Killed            bool
	Untraversed       []int
	Inbox             chan Message
	TaskChan          chan []int
	DoneChan          chan struct{}
	OrdinaryMap       map[int]chan Message
	Ctx               context.Context
	Cancel            context.CancelFunc
	TotalMessages     *int64
	LeadersElected    *int32
	SuccessfulReturns int
}

func (c *CandidateProcess) RunCandidate() {
	for {
		select {
		case <-c.Ctx.Done():
			return
		case nodes, ok := <-c.TaskChan:
			if !ok {
				return
			}

			c.Untraversed = nodes

			c.executeTraversal()

			c.DoneChan <- struct{}{}
		}
	}
}

func (c *CandidateProcess) executeTraversal() {
	for len(c.Untraversed) > 0 {
		idx := rand.Intn(len(c.Untraversed))
		l := c.Untraversed[idx]

		fmt.Printf("[KANDYDAT %d] Wysyła wiadomość (poziom %d) do węzła zwykłego %d\n", c.ID, c.Level, l)
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

					c.SuccessfulReturns++
					fmt.Printf("[KANDYDAT %d] powrót z węzła %d\n", c.ID, msg.SourceLink)

					c.Untraversed = append(c.Untraversed[:idx], c.Untraversed[idx+1:]...)

					break
				}

				if (msg.Level < c.Level) || (msg.Level == c.Level && msg.SenderID < c.ID) || c.Killed {
					fmt.Printf("[KANDYDAT %d] ignoruje (senderID: %d)\n", c.ID, msg.SenderID)
					continue
				}

				c.Killed = true
				fmt.Printf("[ŚMIERĆ KANDYDATA] Kandydat %d został pokonany przez Kandydata %d\n",
					c.ID, msg.SenderID)

				if targetChan, ok := c.OrdinaryMap[msg.SourceLink]; ok {
					targetChan <- msg
					atomic.AddInt64(c.TotalMessages, 1)
				}
			}
		}
	}
}

func generateRange(start, end int) []int {
	var list []int
	for i := start; i < end; i++ {
		list = append(list, i)
	}
	return list
}

func runScenario(n int) {
	var totalMessages int64 = 0
	var leadersElected int32 = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ordinaryChannels := make(map[int]chan Message, n)
	candidateChannels := make(map[int]chan Message)

	bufferSize := n * 50
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
			p.RunOrdinary()
		}(op)
	}

	activeIDs := []int{0, 1, 2, 3}
	candidates := make(map[int]*CandidateProcess)

	for _, id := range activeIDs {
		nodeStates[id].IsCandidate = true
		nodeStates[id].HasReceivedMessage = true

		c := &CandidateProcess{
			ID:                id,
			Level:             0,
			Killed:            false,
			Untraversed:       nil,
			Inbox:             candidateChannels[id],
			TaskChan:          make(chan []int, 10),
			DoneChan:          make(chan struct{}, 10),
			OrdinaryMap:       ordinaryChannels,
			Ctx:               ctx,
			Cancel:            cancel,
			TotalMessages:     &totalMessages,
			LeadersElected:    &leadersElected,
			SuccessfulReturns: 0,
		}
		candidates[id] = c

		go c.RunCandidate()
	}

	fmt.Println("\n--- FAZA 1 ---")
	phase1Tasks := map[int][]int{
		0: {0},
		1: {1},
		2: {2},
		3: generateRange(3, n),
	}
	for id, nodes := range phase1Tasks {
		candidates[id].TaskChan <- nodes
	}
	for _, id := range activeIDs {
		<-candidates[id].DoneChan
	}

	fmt.Println("\n--- FAZA 2 ---")
	phase2Tasks := map[int][]int{
		0: {n - 1},
		1: {n - 1},
		2: {0},
		3: {1},
	}
	for id, nodes := range phase2Tasks {
		candidates[id].TaskChan <- nodes
	}
	for _, id := range []int{2, 3} {
		<-candidates[id].DoneChan
	}

	fmt.Println("\n--- FAZA 3 ---")
	phase3Tasks := map[int][]int{
		2: append([]int{1}, generateRange(3, n)...),
		3: {0, 2},
	}
	for id, nodes := range phase3Tasks {
		candidates[id].TaskChan <- nodes
	}
	for _, id := range []int{2, 3} {
		<-candidates[id].DoneChan
	}

	for id, c := range candidates {
		fmt.Printf("Kandydat %d -> Żyje: %t, Udane przejęcia: %d\n", id, !c.Killed, c.SuccessfulReturns)

		if !c.Killed && c.SuccessfulReturns == n {
			atomic.AddInt32(&leadersElected, 1)
		}
	}

	fmt.Printf("Liczba wybranych liderów: %d\n", atomic.LoadInt32(&leadersElected))
	fmt.Printf("Łączna liczba wysłanych wiadomości: %d\n", atomic.LoadInt64(&totalMessages))

	cancel()

	for i := range n {
		close(ordinaryChannels[i])
	}

	ordinaryWg.Wait()
}

func main() {
	n := 10
	runScenario(n)
}
