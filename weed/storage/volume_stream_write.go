package storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

func (v *Volume) StreamWrite(n *needle.Needle, data io.Reader, dataSize uint32) (err error) {

	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	df, ok := v.DataBackend.(*backend.DiskFile)
	if !ok {
		return fmt.Errorf("unexpected volume backend")
	}
	offset, _, _ := v.DataBackend.GetStat()

	header := make([]byte, NeedleHeaderSize+TimestampSize) // adding timestamp to reuse it and avoid extra allocation
	CookieToBytes(header[0:CookieSize], n.Cookie)
	NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)
	n.Size = 4 + Size(dataSize) + 1
	SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)

	n.DataSize = dataSize

	// needle header
	df.Write(header[0:NeedleHeaderSize])

	// data size and data
	util.Uint32toBytes(header[0:4], n.DataSize)
	df.Write(header[0:4])
	// write and calculate CRC
	crcWriter := needle.NewCRCwriter(df)
	io.Copy(crcWriter, io.LimitReader(data, int64(dataSize)))

	// flags
	util.Uint8toBytes(header[0:1], n.Flags)
	df.Write(header[0:1])

	// data checksum
	util.Uint32toBytes(header[0:needle.NeedleChecksumSize], crcWriter.Sum())
	df.Write(header[0:needle.NeedleChecksumSize])

	// write timestamp, padding
	n.AppendAtNs = uint64(time.Now().UnixNano())
	util.Uint64toBytes(header[needle.NeedleChecksumSize:needle.NeedleChecksumSize+TimestampSize], n.AppendAtNs)
	padding := needle.PaddingLength(n.Size, needle.Version3)
	df.Write(header[0 : needle.NeedleChecksumSize+TimestampSize+padding])

	// add to needle map
	if err = v.nm.Put(n.Id, ToOffset(int64(offset)), n.Size); err != nil {
		glog.V(4).Infof("failed to save in needle map %d: %v", n.Id, err)
	}
	return
}
