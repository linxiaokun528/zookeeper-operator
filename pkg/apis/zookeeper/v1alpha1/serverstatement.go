package v1alpha1

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	RoleParticipant = "participant"
	RoleObserver    = "observer"
)

// TODO: make an instance of ServerStatement a member of struct Member when we allow users to specify roles.

// A server statement is like
// server.<positive id> = <address>:<quorum port>:<leader election port>:<role>;<client port ip>:<client port>
// In this controller, the <quorum port> is 2888, <leader election port> is 3888,
// <client port address> is 0.0.0.0 and <client port> is 2181
type ServerStatement struct {
	Id int

	Address            string
	QuorumPort         int
	LeaderElectionPort int

	Role string

	ClientPortAddress string
	ClientPort        int
}

func NewServerStatementFromString(serverStatement string) *ServerStatement {
	re, err := regexp.Compile("server.(\\d+)=(.+):(\\d+):(\\d+):(.+);(.+):(\\d+)")
	if err != nil {
		panic(err)
	}

	match := re.FindStringSubmatch(serverStatement)

	if len(match) != 8 {
		panic(fmt.Errorf("Invalid serverStatement: %s", serverStatement))
	}

	id, err := strconv.Atoi(match[1])
	if err != nil {
		panic(fmt.Errorf("Unable to get the id from serverStatement (%s): %v", serverStatement, err))
	}
	quorumPort, err := strconv.Atoi(match[3])
	if err != nil {
		panic(fmt.Errorf("Unable to get the quorumPort from serverStatement (%s): %v", serverStatement, err))
	}
	leaderElectionPort, err := strconv.Atoi(match[4])
	if err != nil {
		panic(fmt.Errorf("Unable to get the leaderElectionPort from serverStatement (%s): %v",
			serverStatement, err))
	}
	role := strings.ToLower(match[5])
	if role != RoleParticipant && role != RoleObserver {
		panic(fmt.Errorf("Invalid role value for zookeeper cluster: %s. Only %s or %s is expected.",
			role, RoleParticipant, RoleObserver))
	}
	clientPort, err := strconv.Atoi(match[7])
	if err != nil {
		panic(fmt.Errorf("Unable to get the clientPort from serverStatement (%s): %v",
			serverStatement, err))
	}

	return &ServerStatement{
		Id:                 id,
		Address:            match[2],
		QuorumPort:         quorumPort,
		LeaderElectionPort: leaderElectionPort,
		Role:               role,
		ClientPortAddress:  match[6],
		ClientPort:         clientPort,
	}
}

func (s ServerStatement) String() string {
	return fmt.Sprintf("server.%d=%s:%d:%d:%s;%s:%d", s.Id, s.Address, s.QuorumPort,
		s.LeaderElectionPort, s.Role, s.ClientPortAddress, s.ClientPort)
}
