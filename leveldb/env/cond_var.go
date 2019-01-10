package env

import "sync"

type CondVar struct {
	cv *sync.Cond
	mu *sync.Mutex
}

func NewCondVar() *CondVar {
	mu := &sync.Mutex{}
	c := &CondVar{mu: mu, cv: sync.NewCond(mu)}
	mu.Lock()
	return c
}

func (cv *CondVar) Wait() {
	cv.cv.Wait()
}

func (cv *CondVar) Signal() {
	cv.cv.Signal()
}

func (cv *CondVar) SignalAll() {
	cv.cv.Broadcast()
}
