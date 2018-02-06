// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"fmt"
	"time"
)

// information for message to be sent to the server to keep track of current back off
type dataMessage struct {
	message				Message
	currentBackOff		int
	waitEpoches			int
}

type client struct {
	// TODO: implement this!
	establishedConn			chan bool
	connID					int
	sequenceNumber			int
	connection				*lspnet.UDPConn
	closedConn				chan int      		// indicate whether the connection is closed.
	write					chan []byte    	// Write()
	sendMessageQueue		*list.List    	// store the sending message
	messageFromServer		chan Message	// message received from the server - message or ack
	receivedAckCount		int				// record the index of the request which already received the ack from the server
	slidingWindowSize		int
	sendSlidingWindow		map[int]bool		// keep track of the sliding window for sending message
	receiveMessageQueue		*list.List		// store the received message
	sentAckCount			int 			// record the number of the ack which has been sent to the server
	receiveSlidingWindow	map[int]bool		// keep track of the sliding window for processing out of order messages
	sendMesToServer			chan []byte
	read					chan Message	//
	close					chan int
	closeResponse			chan bool
	prepareToClose			bool
	mainSignal				chan int
	epochSignal				chan int
	readSignal				chan int
	
	epochLimit				int				// 
	epochMillis				int
	newEpoch				chan int
	epochCount				int				// total number of epoches has passed
	maxBackOffInterval		int
	connBackOff				int				// record currentBackOff for connection request
	connWaitedEpoch			int				// record the epoch times for the conncetion request since last sending
	
	test					chan int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
//	fmt.Println("Entering New Client")
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		fmt.Println("Something wrong when resolving udp address")
		return nil, err
	}
	udpConn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Println("Something wrong when resolving udp address")
		return nil, err
	}

	
	c := &client {
		establishedConn:		make(chan bool),
		connID:					0,
		sequenceNumber:			1,
		connection:				udpConn,
		closedConn:				make(chan int),
		write:					make(chan []byte),
		sendMessageQueue:		list.New(),
		messageFromServer:		make(chan Message),
		receivedAckCount:		0,
		slidingWindowSize:		params.WindowSize,
		sendSlidingWindow:		make(map[int]bool),
		receiveMessageQueue:	list.New(),
		sentAckCount:			0,
		receiveSlidingWindow:	make(map[int]bool),
		sendMesToServer:		make(chan []byte),
		read:					make(chan Message, 3000),
		close:					make(chan int),
		closeResponse:			make(chan bool),
		prepareToClose:			false,
		mainSignal:				make(chan int),
		epochSignal:			make(chan int),
		readSignal:				make(chan int),
		epochLimit:				params.EpochLimit,
		epochMillis:			params.EpochMillis,
		newEpoch:				make(chan int),
		epochCount:				0,
		maxBackOffInterval:		params.MaxBackOffInterval,
		connBackOff:			0,
		connWaitedEpoch:		0,
		test:					make(chan int, 2),
	}
	
	

	go MainRoutine(c)
	message := NewConnect()
	buffer, _ := json.Marshal(message)
	c.connection.Write(buffer)
	go ReadRoutine(c)
	for {
		select {
			case <-c.establishedConn:
				return c, nil
		}
	}
}

func MainRoutine(c *client) {
//	fmt.Println("Entering main routine")
	go StartEpoch(c)
	for {
		
		select {
			case <-c.mainSignal:
				return
			case buffer := <-c.write:      // send message to server
				newMessage := NewData(c.connID, c.sequenceNumber, len(buffer), buffer)
				dataMes := &dataMessage {
					message:			*newMessage,
					currentBackOff:		0,
					waitEpoches:		0,
				}
				c.sendMessageQueue.PushBack(dataMes)
				c.sequenceNumber++
//				fmt.Println("sequence number: ", c.sequenceNumber)
				
				// if the sliding window is not full, send the message to server and record the ack status
				if c.slidingWindowSize > len(c.sendSlidingWindow) {
					c.sendSlidingWindow[newMessage.SeqNum]= false
					buffer, err := json.Marshal(newMessage)
					if err != nil {
						fmt.Printf("Wrong message: %s.\n", buffer)
					}
					c.connection.Write(buffer)
				}
				
			case message := <-c.messageFromServer:
				c.epochCount = 0
				if message.Size > len(message.Payload) {
					continue
				} else if message.Size <= len(message.Payload) {
					message.Payload = message.Payload[:message.Size]
					
					if message.Type == MsgAck {			// Ack
						if message.SeqNum == 0 {
							if c.connID == 0 {
								c.connID = message.ConnID
								c.establishedConn <- true
							} else {
								c.epochCount = 0
							} 		
						}
						if message.SeqNum > c.receivedAckCount {	// ack for message
							c.sendSlidingWindow[message.SeqNum] = true
							var index int
							for index = c.receivedAckCount + 1; index <= len(c.sendSlidingWindow) + c.receivedAckCount; index++ {
								if c.sendSlidingWindow[index] == false {
									break				// does not receive the older ack
								}
								c.sendMessageQueue.Remove(c.sendMessageQueue.Front())  // remove the corresponding sent message from the queue
								delete(c.sendSlidingWindow, index) // shift the sliding window
							}
							c.receivedAckCount = index - 1
							
							index = len(c.sendSlidingWindow)
							for element := c.sendMessageQueue.Front(); index < c.slidingWindowSize && element != nil; element = element.Next() {
								c.sendSlidingWindow[index + c.receivedAckCount + 1] = false
								mes := element.Value.(*dataMessage).message
								buffer, _ := json.Marshal(mes)
								c.connection.Write(buffer)
								index++
							}
						}
					} else {		// data message	
						isExisted := c.receiveSlidingWindow[message.SeqNum]
						if !isExisted {
							c.receiveMessageQueue.PushBack(message)
	//						fmt.Printf("ID: %d DealServerMessage:: len of RMQ: %d\n", c.connID, c.receiveMessageQueue.Len())
							c.receiveSlidingWindow[message.SeqNum] = true
						}
						// Messages must be acknowledged when received.
							mes := NewAck(c.connID, message.SeqNum)
							buffer, _ := json.Marshal(mes)
							c.connection.Write(buffer)
							var i int
							for i = 0; i < len(c.receiveSlidingWindow); i++ {
								isReceived := c.receiveSlidingWindow[i + c.sentAckCount + 1]
								if !isReceived {
									break
								}
								seqNum := c.sentAckCount + i + 1
								element := c.receiveMessageQueue.Front()
								for element != nil {
									if element.Value.(Message).SeqNum == seqNum {
										c.read <- element.Value.(Message)
										c.receiveMessageQueue.Remove(element)
										delete(c.receiveSlidingWindow, seqNum)  	// shift the sliding window
										break
									}
									element = element.Next()
								}
							}
							c.sentAckCount += i
					}
				}
				
			case <-c.newEpoch:
				c.epochCount++


				if c.epochCount >= c.epochLimit {
					if c.prepareToClose {
						c.closeResponse <- false
					} else {
						c.closedConn <- 1
						c.connection.Close()
					}
				} else if c.prepareToClose && c.sendMessageQueue.Len() == 0 && len(c.sendSlidingWindow) == 0 {
					c.closeResponse <- true
				} else {
					resendData := false
					if c.connID == 0 {			// reconnection
						if c.connWaitedEpoch < c.connBackOff {  	
							c.connWaitedEpoch++
						} else {		// reached the amount of epoches to resend the data message
							mes := NewConnect()
							buf, _ := json.Marshal(mes)
							c.connection.Write(buf)
							resendData = true
							c.connWaitedEpoch = 0			// reset the amount of waiting epoches 
							if c.connBackOff >= c.maxBackOffInterval {
								c.connBackOff = c.maxBackOffInterval
							} else {
								if c.connBackOff == 0 {			// exponential back-off
									c.connBackOff = 1
								} else {
									c.connBackOff = c.connBackOff << 1
								}
							}
						}
					} else {	
						for num, received := range c.sendSlidingWindow {
							if !received {
								element := c.sendMessageQueue.Front()
								for element != nil {
									mes := element.Value.(*dataMessage)
									
									if mes.message.SeqNum == num {
										if mes.waitEpoches >= mes.currentBackOff {
											buf, _ := json.Marshal(mes.message)
											c.connection.Write(buf)
											resendData = true
											
											// update current back-off
											if mes.currentBackOff == 0 {
												mes.currentBackOff = 1
											} else {
												mes.currentBackOff = mes.currentBackOff * 2
											}
											if mes.currentBackOff >= c.maxBackOffInterval {
												mes.currentBackOff = c.maxBackOffInterval
											}
											mes.waitEpoches = 0
											
										} else {
											mes.waitEpoches++
										}
										element.Value.(*dataMessage).currentBackOff = mes.currentBackOff
										element.Value.(*dataMessage).waitEpoches = mes.waitEpoches
										break
									}
									element = element.Next()
								}
							}
						}
						
						if c.epochCount != 0 && !resendData {
							// send ack with sequence number 0 to server
							mes := NewAck(c.connID, 0)
							buf, _ := json.Marshal(mes)
							c.connection.Write(buf)
						}
					}
				}
			
			case <-c.test:
				if c.sendMessageQueue.Len() == 0 && len(c.sendSlidingWindow) == 0 && c.prepareToClose {
					c.closeResponse <- true
				}
			
			case <-c.close:
				c.prepareToClose = true
				c.test <- 1
		}
	}
}


func StartEpoch(c *client) error {
	tick := time.Tick(time.Duration(c.epochMillis) * time.Millisecond) // trigger a channel
	for {
		select {
			case <-c.epochSignal:
				return nil
			case <-tick:			// epoch is triggered
				c.newEpoch <- 1			
		}
	}
	return nil
}


func ReadRoutine(c *client) {
//	fmt.Println("Entering read routine")
	var buf [1500]byte
	var message Message
	
	for {
		select {
			case <-c.readSignal:
				return
			default:
				n, err := c.connection.Read(buf[0:])	
				
				if err != nil {
					continue
				}
				if n != 0 {
					json.Unmarshal(buf[0:n], &message)
					c.messageFromServer <- message
				}
		}
	}
}

func WriteRoutine(c *client) {
//	fmt.Println("Entering write routine")
	for {
		select {
			case buffer := <-c.sendMesToServer:
//				fmt.Printf("ask for connection %s\n", buffer)
				if len(buffer) > 0 {
					c.connection.Write(buffer)
				}
		}
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {
		case <-c.closedConn:
			return nil, errors.New("the connection is closed")
		case message := <-c.read:
			return message.Payload, nil
	}
	
}

func (c *client) Write(payload []byte) error {
	c.write <- payload
	return nil
}

func (c *client) Close() error {
	c.close <- 1
	select {
		case canClose := <-c.closeResponse:
			if canClose {
				c.mainSignal <- 1
				c.epochSignal <- 1
				return nil
			} else {
				return errors.New("not all messages are sent or get ack\n")
			}
	}
}

func CloseEverything(c *client) {
	c.closedConn <- 1
//	close(c.write)
	c.connection.Close()
//	close(c.read)
}

