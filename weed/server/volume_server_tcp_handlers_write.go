package weed_server

import (
	"bufio"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
	"net"
	"strings"
)

func (vs *VolumeServer) HandleTcpConnection(c net.Conn) {
	defer c.Close()

	glog.V(0).Infof("Serving writes from %s", c.RemoteAddr().String())

	bufReader := bufio.NewReaderSize(c, 4*1024*1024)

	for {
		cmd, err := bufReader.ReadString('\n')
		if err != nil {
			glog.Errorf("read command from %s: %v", c.RemoteAddr().String(), err)
			return
		}
		switch cmd[0] {
		case '+':
			err = vs.handleTcpPut(cmd, bufReader)
		case '-':
			err = nil
		}

		if err == nil {
			c.Write([]byte("+OK\n"))
		} else {
			c.Write([]byte("-ERR " + string(err.Error()) + "\n"))
		}
	}

}

func (vs *VolumeServer) handleTcpPut(cmd string, bufReader *bufio.Reader) (err error) {

	fileId := cmd[1:]

	commaIndex := strings.LastIndex(fileId, ",")
	if commaIndex <= 0 {
		return fmt.Errorf("unknown fileId %s", fileId)
	}

	vid, fid := fileId[0:commaIndex], fileId[commaIndex+1:]

	volumeId, ve := needle.NewVolumeId(vid)
	if ve != nil {
		return fmt.Errorf("unknown volume id in fileId %s", fileId)
	}

	n := new(needle.Needle)
	n.ParsePath(fid)

	volume := vs.store.GetVolume(volumeId)
	if volume == nil {
		return fmt.Errorf("volume %d not found", volumeId)
	}

	sizeBuf := make([]byte, 4)
	if _, err = bufReader.Read(sizeBuf); err != nil {
		return err
	}
	dataSize := util.BytesToUint32(sizeBuf)

	err = volume.StreamWrite(n, bufReader, dataSize)
	if err != nil {
		return err
	}

	return nil
}
