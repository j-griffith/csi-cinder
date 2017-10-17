package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/codedellemc/gocsi"
	"github.com/codedellemc/gocsi/csi"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v2/volumes"
	"github.com/gophercloud/gophercloud/pagination"
)

type Service interface {
	csi.ControllerServer
	csi.IdentityServer
	csi.NodeServer
}

type service struct {
	client *gophercloud.ServiceClient
	mutex  *sync.Mutex
}

func New(endpoint, user, password, tenantID, domainName string) Service {
	opts := gophercloud.AuthOptions{
		IdentityEndpoint: endpoint,
		Username:         user,
		Password:         password,
		TenantID:         tenantID,
		DomainName:       domainName,
	}
	provider, err := openstack.AuthenticatedClient(opts)
	if err != nil {
		fmt.Printf("error creating new cinder service: %+v", err)
		return nil
	}

	eopts := gophercloud.EndpointOpts{
		Region: "RegionOne",
	}

	client, _ := openstack.NewBlockStorageV2(provider, eopts)

	return service{
		client: client,
	}
}

func (s *service) getVolumeByName(name string) (volumes.Volume, error) {
	opts := volumes.ListOpts{Name: name}
	vols := volumes.List(s.client, opts)
	var vol volumes.Volume
	err := vols.EachPage(func(page pagination.Page) (bool, error) {
		vList, err := volumes.ExtractVolumes(page)
		if err != nil {
			return false, err
		}
		for _, v := range vList {
			if v.Name == name {
				vol = v
				return true, nil
			}
		}
		// Not found
		return false, nil
	})

	if err != nil {
		return volumes.Volume{}, err
	}
	return vol, nil
}

func (s *service) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	// Create if DNE, else return existing volume, for now we're just
	// going off of name, future, build a map to UUID, use metadata?
	// keeping in mind Cinder doesn't guarantee uniqueness on name
	name := req.Name
	v, err := s.getVolumeByName(name)
	if err != nil {
		return gocsi.ErrCreateVolume(csi.Error_CreateVolumeError_UNKNOWN,
			"CreateVolume failed"), nil
	}
	if (volumes.Volume{}) == v {
		opts := volumes.CreateOpts{}
		// NOTE(jdg): I know why we have a `range` but still find it awkward
		// should bring this up again in CSI spec.

		// FIXME(jdg): hardcoding a size for POC here
		opts.Size = 1
		opts.VolumeType = req.Parameters["vtype"]
		opts.Description = "CSI Volume"
		opts.Name = name
		_, err = volumes.Create(s.client, opts).Extract()
		if err != nil {
			return volume.Response{Err: err.Error()}
		}
		path := filepath.Join(s.Conf.MountPoint, r.Name)
		if err := os.Mkdir(path, os.ModeDir); err != nil {
			return volume.Response{Err: err.Error()}
		}
		return volume.Response{}

	}

	return &csi.CreateVolumeResponse{
		Reply: &csi.CreateVolumeResponse_Result_{
			Result: &csi.CreateVolumeResponse_Result{
				VolumeInfo: v,
			},
		},
	}, nil
}
