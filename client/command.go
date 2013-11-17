package client

import (
	"bytes"
	"fmt"
)

type dldbCommand struct {
	name   string
	opCode int
	// isModified bool // true is command that need to update db, false is not
	argc int //argc is 0 if it is not sure
}

// type commandFunc func(*DldbClient) error

type dldbCommands []*dldbCommand //find the command according to the opCode
var Commands dldbCommands

type commandDict map[string]int

var CommandDict commandDict

func initCommandTable() {
	Commands = make([]*dldbCommand, 3)

	//parameter 2: keysize and key value, for example: get name
	getCmd := &dldbCommand{
		"GET", 1, 2,
	}
	//parameter 3: keysize and key value, for example: set name lizhe
	setCmd := &dldbCommand{
		"SET", 2, 3,
	}

	Commands[getCmd.opCode] = getCmd
	Commands[setCmd.opCode] = setCmd

	CommandDict = make(map[string]int)
	CommandDict[getCmd.name] = getCmd.opCode
	CommandDict[setCmd.name] = setCmd.opCode
}

func isValidCommand(client *DldbClient) bool {
	commandName := string(bytes.ToUpper(client.argv[0]))
	opCode, ok := CommandDict[commandName]
	if !ok {
		fmt.Printf("Unknown command %s\n", commandName)
		return false
	}
	client.opCode = opCode
	command := Commands[opCode]
	// judge argc -1 means variant length ignore it
	if command.argc != -1 && command.argc != len(client.argv) {
		// command argc doesn't match client argc, invalid request
		fmt.Printf("invalid argc, argc = %d, want %d\n", len(client.argv), command.argc)
		return false
	}
	return true
}

func init() {
	initCommandTable()
}

/*func getError(int code) error {
	switch code {
	case
	}
}*/
