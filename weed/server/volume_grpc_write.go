package weed_server

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) VolumeNeedleWrite(ctx context.Context, req *volume_server_pb.VolumeNeedleWriteRequest) (*volume_server_pb.VolumeNeedleWriteResponse, error) {

	if err := vs.handlePut(req); err != nil {
		glog.Errorf("handle put %s: %v", req.FileId, err)
	}

	return &volume_server_pb.VolumeNeedleWriteResponse{}, nil
}

func (vs *VolumeServer) handlePut(req *volume_server_pb.VolumeNeedleWriteRequest) (err error) {

	volumeId, n, err2 := vs.parseFileId(req.FileId)
	if err2 != nil {
		return err2
	}

	n.Data = req.Data
	n.Checksum = needle.NewCRC(n.Data)

	_, err = vs.store.WriteVolumeNeedle(volumeId, n, false)

	return err

}
