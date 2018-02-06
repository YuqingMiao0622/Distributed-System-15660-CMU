// Contains the implementation of a LSP server.

package lsp

import (
	"time"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
)
type server struct {
	// server listening port
	ln *lspnet.UDPConn
	// all clients
	clients map[int]*client_data
	addr    chan *lspnet.UDPAddr
	mes     chan Message
	// current connection id
	cur_conn_id  int
	close_client chan int
	// send to Write
	send chan Message
	// send to Read
	read               chan Message
	send_error         chan bool
	closing            bool
	closed             chan int
	one_client_closed  chan int
	close_accept       chan int
	close_conn_success chan bool
	is_client_alive    chan int
	client_dead        chan int
	close_all		   chan int
}
type client_data struct {
	// connection id
	connId int
	ack    chan Message
	data   chan Message
	//
	addr *lspnet.UDPAddr
	// epoch happen
	epoch chan int
	// for closing connection
	close_conn     chan int
	write          chan Message
	send_index     int
	send_window    map[int]bool
	send_queue     *list.List
	epoch_passed   int
	receive_index  int
	receive_window map[int]bool
	backoff_limit  map[int]int
	backoff_currnt map[int]int
	receive_queue  *list.List
	// the next
	next_write_seqnum int
	close_epoch       chan int
	closing_commu     chan int
	closing            bool
}

var epoch_interval int = -1
var epoch_limit int = -1
var window_size int = -1
var maxBackOffInterval int = -1

const DEFAULT_SEQNUM int = 0

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	str := lspnet.JoinHostPort("localhost", strconv.Itoa(port))
	ln, err := lspnet.ResolveUDPAddr("udp", str)
	if err != nil {
		fmt.Println("resolving error")
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", ln)
	if err != nil {
		fmt.Println("listening error")
		return nil, err
	}
	epoch_interval = params.EpochMillis
	epoch_limit = params.EpochLimit
	window_size = params.WindowSize
	maxBackOffInterval = params.MaxBackOffInterval
	server := &server{
		// server listening port
		ln: conn,
		// all clients
		clients: make(map[int]*client_data),
		addr:    make(chan *lspnet.UDPAddr),
		mes:     make(chan Message, 1),
		// current connection id
		cur_conn_id:  1,
		close_client: make(chan int),
		// send to Write
		send: make(chan Message),
		// send to Read
		read:               make(chan Message, 2500),
		send_error:         make(chan bool),
		closing:            false,
		closed:             make(chan int),
		one_client_closed:  make(chan int, 100),
		close_accept:       make(chan int, 100),
		close_conn_success: make(chan bool),
		is_client_alive:    make(chan int),
		client_dead:        make(chan int, 100),
		close_all:          make(chan int,),
	}
	go accept(server)
	go serve(server)
	return server, nil
}
func serve(s *server) {
	for {
		select {
		case message := <-s.mes:
			if message.Type == MsgData {
				if s.closing {
					continue
				}
				s.clients[message.ConnID].data <- message
			} else if message.Type == MsgAck {
				if _, ok := s.clients[message.ConnID]; !ok {
					continue
				}
				s.clients[message.ConnID].ack <- message
			} else if message.Type == MsgConnect {
				addr := <-s.addr
				if s.closing {
					continue
				}
				client := &client_data{
					//current connction id
					s.cur_conn_id,
					make(chan Message, 10),
					make(chan Message),
					//
					addr,
					// epoch happen
					make(chan int),
					// for closing connection
					make(chan int),
					make(chan Message),
					1,
					make(map[int]bool),
					list.New(),
					0,
					1,
					make(map[int]bool),
					make(map[int]int),
					make(map[int]int),
					list.New(),
					1,
					make(chan int),
					make(chan int),
					false,

				}
				connId := client.connId
				s.cur_conn_id++
				s.clients[connId] = client
				buf, error := json.Marshal(NewAck(connId, 0))
				if error != nil {
					fmt.Println("marshal error")
				}
				s.ln.WriteToUDP(buf, client.addr)
				go serveClient(s, client)
			}
		case mes := <-s.send:
			client, ok := s.clients[mes.ConnID]
			if ok == true {
				client.write <- mes
				s.send_error <- false
			} else {
				s.send_error <- true
			}
		//shut down one by one, may be slow, test later
		// this is for server close function, it will send all pending messages
		case <- s.close_all:
			for _,v := range s.clients {
				v.closing_commu <- 0
			}
		case id := <- s.one_client_closed:
			delete(s.clients, id)
			if len(s.clients) == 0 {
				//s.close_accept <- 0
				s.closed <- 0
				return
			} else {
				continue
			}
		// this is for close conn function, it will close client right away
		case connid := <-s.close_client:
			if val, ok := s.clients[connid]; !ok {
				s.close_conn_success <- false
			} else {
				s.close_conn_success <- true
				val.close_conn <- 0
				continue
			}
		}
	}

}
func serveClient(s *server, c *client_data) {
	go epoch(c)
	for {
		select {
		case data := <-c.data:
			c.epoch_passed = 0
			// receive index is the up to the one that has been comfirmed in client
			if len(data.Payload) < data.Size {
				continue
			} else if len(data.Payload) > data.Size{
				data.Payload = data.Payload[:data.Size]
			}
			if data.SeqNum < c.receive_index {
				ack := NewAck(c.connId, data.SeqNum)
				buf, error := json.Marshal(ack)
				if error != nil {
					fmt.Println("error in marshal in receiving data")
				}
				s.ln.WriteToUDP(buf, c.addr)
				continue
			}
			if _, ok := c.receive_window[data.SeqNum]; !ok {
				ack := NewAck(c.connId, data.SeqNum)
				buf, error := json.Marshal(ack)
				if error != nil {
					fmt.Println("error in marshal in receiving data")
				}
				s.ln.WriteToUDP(buf, c.addr)
				c.receive_queue.PushBack(data)
				c.receive_window[data.SeqNum] = true
				begin := c.receive_index
				end := len(c.receive_window)
				for i := begin; i < begin+end; i++ {
					if c.receive_window[i] == false {
						break
					} else {
						c.receive_index++
						delete(c.receive_window, i)
						ptr := c.receive_queue.Front()
						len := c.receive_queue.Len()
						for j := 0; j < len; j++ {
							if ptr.Value.(Message).SeqNum != i {
								ptr = ptr.Next()
								continue
							} else {
								s.read <- ptr.Value.(Message)
								c.receive_queue.Remove(ptr)
								break
							}
						}

					}
				}
			} else {
				ack := NewAck(c.connId, data.SeqNum)
				buf, error := json.Marshal(ack)
				if error != nil {
					fmt.Println("error in marshal in receiving data")
				}
				s.ln.WriteToUDP(buf, c.addr)
			}

		// received a acknowledgement
		case ack := <-c.ack:
			c.epoch_passed = 0
			if ack.SeqNum >= c.send_index {
				c.send_window[ack.SeqNum] = true
				begin := c.send_index
				end := len(c.send_window)
				for i := begin; i < begin+end; i++ {
					if c.send_window[i] == false {
						break
					} else {
						c.send_index++
						delete(c.send_window, i)
						delete(c.backoff_limit, i)
						delete(c.backoff_currnt, i)
						c.send_queue.Remove(c.send_queue.Front())
					}
				}
				begin = c.send_index
				end = begin + window_size
				ptr := c.send_queue.Front()
				for i := begin; i < end; i++ {
					if _, ok := c.send_window[i]; ok {
						continue
					} else {
						if ptr != nil {
							c.send_window[i] = false
							c.backoff_currnt[i] = 0
							c.backoff_limit[i] = 0
							buf, error := json.Marshal(ptr.Value.(Message))
							if error != nil {
								fmt.Println("marshal error")
							}
							s.ln.WriteToUDP(buf, c.addr)
							ptr = ptr.Next()
						} else {
							break
						}
					}
				}
			} else if ack.SeqNum == 0 {
				continue
			}
		case write_mes := <-c.write:
			write_mes.SeqNum = c.next_write_seqnum
			c.send_queue.PushBack(write_mes)
			c.next_write_seqnum++
			if len(c.send_window) < window_size {
				c.send_window[write_mes.SeqNum] = false
				// may have fault
				c.backoff_currnt[write_mes.SeqNum] = 0
				c.backoff_limit[write_mes.SeqNum] = 0
				buf, error := json.Marshal(write_mes)
				if error != nil {
					fmt.Println("marshal error")
				}
				s.ln.WriteToUDP(buf, c.addr)
			}

		case <- c.epoch:
			if c.epoch_passed != 0 {
				ack := NewAck(c.connId, 0)
				buf, error := json.Marshal(ack)
				if error != nil {
					fmt.Println("error in marshal in receiving data")
				}
				s.ln.WriteToUDP(buf, c.addr)
			}
			c.epoch_passed++
			if c.closing == true {
				if len(c.send_window) == 0 && len(c.receive_window) == 0 {
					s.one_client_closed <- c.connId
					// might have problem
					return
				}
			}
			if c.epoch_passed < epoch_limit {
				for i, v := range c.send_window {
					if v == true {
						continue
					} else {
						ptr := c.send_queue.Front()
						for ptr != nil {
							if ptr.Value.(Message).SeqNum == i {
								if c.backoff_limit[i] == 0 {
									c.backoff_limit[i] = 1
								} else {
									if c.backoff_limit[i] != c.backoff_currnt[i] {
										c.backoff_currnt[i]++
										ptr = ptr.Next()
										continue
									} else {
										c.backoff_currnt[i] = 0
										if c.backoff_limit[i] != maxBackOffInterval {
											c.backoff_limit[i] *= 2
										}
									}
								}
								buf, error := json.Marshal(ptr.Value.(Message))
								if error != nil {
									fmt.Println("marshal error")
								}
								s.ln.WriteToUDP(buf, c.addr)
								ptr = ptr.Next()
							} else {
								ptr = ptr.Next()
								continue
							}
						}
					}
				}
			} else {
				go close_client(c)
				continue
			}
		case <-c.close_conn:
			c.close_epoch <- 0
			s.client_dead <- 0
			s.one_client_closed <- c.connId
			return
		case <- c.closing_commu:
			c.closing = true
		}
	}
}
func close_client(c *client_data) {
	c.close_conn <- 0
	return
}
func accept(s *server) {
	for {
		select {
		case <-s.close_accept:
			return
		default:
			var buffer [2000]byte
			n, addr, error := s.ln.ReadFromUDP(buffer[0:])
			if error != nil {
				fmt.Println("Read error")
				continue
			}
			var message Message
			error = json.Unmarshal(buffer[0:n], &message)
			if error != nil {
				fmt.Println("unmarshal error")
				continue
			}
			s.mes <- message
			if message.Type == MsgConnect {
				s.addr <- addr
			}
		}
	}
}

func epoch(c *client_data) {
	dur := time.Duration(epoch_interval)
	epoch := time.Tick(dur * time.Millisecond)
	for {
		select {
		case <- c.close_epoch:
			return
		case <- epoch:
			c.epoch <- 0
		}
	}
}
func (s *server) Read() (int, []byte, error) {
	select{
		case mes := <-s.read:
			return mes.ConnID, mes.Payload, nil
		case <- s.client_dead:
			return 0, nil, errors.New("client is closed")
	}

}

func (s *server) Write(connID int, payload []byte) error {
	mes := NewData(connID, DEFAULT_SEQNUM, len(payload), payload)
	s.send <- *mes
	select {
	default:
		success := <-s.send_error
		if success == false {
			return nil
		}
		return errors.New("connID not exists")
	}
}

func (s *server) CloseConn(connID int) error {
	s.close_client <- connID
	tf := <-s.close_conn_success
	if tf {
		return nil
	} else {
		return errors.New("no connid found")
	}
}

func (s *server) Close() error {
	s.close_all <- 0
	<-s.closed
	return nil
}
