package tribserver

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"
)

type tribServer struct {
	lib libstore.Libstore
}

type ByTime []tribrpc.Tribble

func (a ByTime) Len() int {
	return len(a)
}

func (a ByTime) Less(i, j int) bool {
	if a[j].Posted.Before(a[i].Posted) {
		return true
	} else {
		return false
	}
}

func (a ByTime) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	lib, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, err
	}
	tribServer := &tribServer{
		lib,
	}
	// question
	rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	user_key := util.FormatUserKey(args.UserID)
	_, err := ts.lib.Get(user_key)
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	} else {
		ts.lib.Put(user_key, user_key)
		reply.Status = tribrpc.OK
		return nil
	}
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	user_key := util.FormatUserKey(args.UserID)
	request_key := util.FormatUserKey(args.TargetUserID)
	_, err := ts.lib.Get(user_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.lib.Get(request_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	err = ts.lib.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err == nil {
		reply.Status = tribrpc.OK
		return nil
	} else {
		reply.Status = tribrpc.Exists
		return nil
	}
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	user_key := util.FormatUserKey(args.UserID)
	request_key := util.FormatUserKey(args.TargetUserID)
	_, err := ts.lib.Get(user_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.lib.Get(request_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	// might have bug
	err = ts.lib.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil

}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	user_key := util.FormatUserKey(args.UserID)
	_, err := ts.lib.Get(user_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	user_key = util.FormatSubListKey(args.UserID)
	list, err := ts.lib.GetList(user_key)
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}
	for i := 0; i < len(list); i++ {
		other_list, err := ts.lib.GetList(util.FormatSubListKey(list[i]))
		if err != nil {
			continue
		}
		for j := 0; j < len(other_list); j++ {
			if other_list[j] == args.UserID {
				reply.UserIDs = append(reply.UserIDs, list[i])
				break
			}
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	user_key := util.FormatUserKey(args.UserID)
	_, err := ts.lib.Get(user_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	post := &tribrpc.Tribble{
		args.UserID,
		time.Now(),
		args.Contents,
	}
	// might have problem
	post_key := util.FormatPostKey(args.UserID, time.Now().UTC().UnixNano())
	post_string, err := json.Marshal(post)
	if err != nil {
		fmt.Println("marshal error")
		return err
	}
	err = ts.lib.Put(post_key, string(post_string))
	if err != nil {
		fmt.Println("put post error")
		return err
	}
	err = ts.lib.AppendToList(util.FormatTribListKey(args.UserID), post_key)
	if err != nil {
		fmt.Println("append post error")
		return err
	}
	reply.Status = tribrpc.OK
	reply.PostKey = post_key
	return nil

}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	user_key := util.FormatUserKey(args.UserID)
	_, err := ts.lib.Get(user_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	fmt.Printf("key is %s \n", args.PostKey)
	_, err = ts.lib.Get(args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	ts.lib.Delete(args.PostKey)
	ts.lib.RemoveFromList(util.FormatTribListKey(args.UserID), args.PostKey)
	reply.Status = tribrpc.OK
	return nil

}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	user_key := util.FormatUserKey(args.UserID)
	_, err := ts.lib.Get(user_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	list, err := ts.lib.GetList(util.FormatTribListKey(args.UserID))
	if err != nil {
		fmt.Println("no list")
	}
	var begin int
	if len(list) > 100 {
		begin = len(list) - 100
	} else {
		begin = 0
	}
	for i := len(list) - 1; i >= begin; i-- {
		post_string, err := ts.lib.Get(list[i])
		if err != nil {
			reply.Status = tribrpc.NoSuchPost
			fmt.Println("no such post")
		}
		buf := []byte(post_string)
		var post tribrpc.Tribble
		json.Unmarshal(buf, &post)
		reply.Tribbles = append(reply.Tribbles, post)
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	user_key := util.FormatUserKey(args.UserID)
	_, err := ts.lib.Get(user_key)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	sublist, err := ts.lib.GetList(util.FormatSubListKey(args.UserID))
	if err != nil && len(sublist) != 0 {
		fmt.Println("retrive list error")
	}
	for i := 0; i < len(sublist); i++ {
		// might have problem
		list, _ := ts.lib.GetList(util.FormatTribListKey(sublist[i]))
		for j := 0; j < len(list); j++ {
			var post tribrpc.Tribble
			p, _ := ts.lib.Get(list[j])
			json.Unmarshal([]byte(p), &post)
			reply.Tribbles = append(reply.Tribbles, post)
		}
	}
	sort.Sort(ByTime(reply.Tribbles))
	if len(reply.Tribbles) > 100 {
		reply.Tribbles = reply.Tribbles[:100]
	}
	reply.Status = tribrpc.OK
	return nil
}
