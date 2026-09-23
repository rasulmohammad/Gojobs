package task_queue

import (
	"fmt"
	"github.com/google/uuid"
)

// [] is a slice where each element (*Task) is a pointer to a Task
// [ptr -> TaskA, ptr -> TaskB, ptr -> TaskC...]
type Queue struct {
	tasks []*Task
}



// Enqueue and Dequeue functions
// Enqueue creates the task, stores it into the data structures
func (q *Queue) Enqueue(payload []byte, idemKey string) (uuid.UUID, error) {

	


}

func (q *Queue) Dequeue() {

}

func Ack() {

}

func Nack() {

}