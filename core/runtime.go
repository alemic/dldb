package core

/*import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
)

type dldbRuntime struct {
	table *commandTable
}

func initRuntime(dbCore *dbCore) *dldbRuntime {
	rtn := new(dldbRuntime)
	rtn.table = initCommandTable()
	return rtn
}

func (self *dldbRuntime) execute(client *DldbClient) error {
	// cast []byte to int
	if opCode, err := binary.ReadVarint(bytes.NewBuffer(client.requestHeader.opCode)); err != nil {
		log.Trace("opcode parse error")
		return err
	} else {
		if command, ok := self.table.commands[client.requestHeader.opCode]; !ok { //opcode not exist in the command table
			log.Trace("invalid opcode parse")
			return errors.New("invalid request")
		} else {
			// judge argc -1 means variant length ignore it
			if command.argc != -1 && command.argc != client.argc {
				// command argc doesn't match client argc, invalid request
				log.Trace("invalid argc, argc = %d, want %d", client.argc, command.argc)
				return errors.New("arg num is error")
			}
			if command.isModified {
				self.readExecute(client)
			} else {
				self.writeExecute(client)
			}
		}

	}
}

func (self *dldbRuntime) readExecute(client *DldbClient) {
	engineBalancer := dbCore.GetBalancer("engine")
	engineBalancer.ProposeRequest(client)
}

func (self *dldbRuntime) writeExecute(client *DldbClient) {

}
*/
