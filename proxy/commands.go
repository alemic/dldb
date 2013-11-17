package proxy

/*
	0 GET
	1 SET



*/

const (
	COMMAND_BATCH = 0
	COMMAND_GET   = 1
	COMMAND_SET   = 2
)

type dldbCommand struct {
	name   string
	opCode int
	// execute    commandFunc
	isModified bool // true is command that need to update db, false is not
	argc       int  //argc is 0 if it is not sure
}

// type commandFunc func(*DldbClient) error

type dldbCommands []*dldbCommand //find the command according to the opCode

/*//parameter 2: keysize and key value, for example: get 4 name
func getCommand(client *DldbClient) error {

}

func setCommand(client *DldbClient) error {

}*/

func initCommandTable() dldbCommands {
	commands := make([]*dldbCommand, 3)

	batchCmd := &dldbCommand{
		"BATCH", COMMAND_BATCH, true, -1,
	}

	//parameter 2: keysize and key value, for example: get name
	getCmd := &dldbCommand{
		"GET", COMMAND_GET, false, 2,
	}
	//parameter 3: keysize and key value, for example: set name lizhe
	setCmd := &dldbCommand{
		"SET", COMMAND_SET, true, 3,
	}
	commands[COMMAND_BATCH] = batchCmd
	commands[COMMAND_GET] = getCmd
	commands[COMMAND_SET] = setCmd
	return commands
}
