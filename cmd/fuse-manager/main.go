/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"flag"
	golog "log"
	"net"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/containerd/containerd/log"
	sddaemon "github.com/coreos/go-systemd/v22/daemon"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	"github.com/containerd/stargz-snapshotter/fusemanager"
	pb "github.com/containerd/stargz-snapshotter/fusemanager/api"
)

const (
	defaultStoreAddr  = "/var/lib/containerd-stargz-grpc/fusestore.db"
	defaultSockerAddr = "/run/containerd-stargz-grpc/fuse-manager.sock"
	defaultLogLevel   = logrus.InfoLevel
)

var (
	fuseStoreAddr = flag.String("fusestore", defaultStoreAddr, "address for the fuse-manager's store")
	sockerAddr    = flag.String("socket", defaultSockerAddr, "address for the fuse-manager's gRPC socket")
	logLevel      = flag.String("log-level", defaultLogLevel.String(), "set the logging level [trace, debug, info, warn, error, fatal, panic]")
)

func main() {
	flag.Parse()

	lvl, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		log.L.WithError(err).Fatal("failed to prepare logger")
	}

	logrus.SetLevel(lvl)
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: log.RFC3339NanoFixed,
	})

	ctx := log.WithLogger(context.Background(), log.L)

	golog.SetOutput(log.G(ctx).WriterLevel(logrus.DebugLevel))

	sigCh := make(chan os.Signal, 1)
	errCh := make(chan error, 1)
	server := grpc.NewServer()

	signal.Notify(sigCh, unix.SIGINT, unix.SIGTERM)
	go func() {
		select {
		case sig := <-sigCh:
			log.G(ctx).Infof("Got %v", sig)
			server.Stop()
		case err := <-errCh:
			log.G(ctx).WithError(err).Fatal("failed to run stargz fuse manager")
		}
	}()

	// Prepare the directory for the socket
	if err := os.MkdirAll(filepath.Dir(*sockerAddr), 0700); err != nil {
		log.G(ctx).WithError(err).Error("failed to create directory %q", filepath.Dir(*sockerAddr))
		errCh <- &net.DNSConfigError{}
	}

	// Try to remove the socket file to avoid EADDRINUSE
	if err := os.Remove(*sockerAddr); err != nil && !os.IsNotExist(err) {
		log.G(ctx).WithError(err).Error("failed to remove old socket file")
		errCh <- err
	}

	l, err := net.Listen("unix", *sockerAddr)
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to listen socket")
		errCh <- err
	}

	if os.Getenv("NOTIFY_SOCKET") != "" {
		notified, notifyErr := sddaemon.SdNotify(false, sddaemon.SdNotifyReady)
		log.G(ctx).Debugf("SdNotifyReady notified=%v, err=%v", notified, notifyErr)
	}
	defer func() {
		if os.Getenv("NOTIFY_SOCKET") != "" {
			notified, notifyErr := sddaemon.SdNotify(false, sddaemon.SdNotifyStopping)
			log.G(ctx).Debugf("SdNotifyStopping notified=%v, err=%v", notified, notifyErr)
		}
	}()

	fm, err := fusemanager.NewFuseManager(ctx, *fuseStoreAddr)
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to configure manager server")
		errCh <- err
	}

	pb.RegisterFileSystemServiceServer(server, fm)

	if err := server.Serve(l); err != nil {
		log.G(ctx).WithError(err).Error("failed to serve fuse manager")
		errCh <- err
	}

	err = fm.Close(ctx)
	if err != nil {
		log.G(ctx).WithError(err).Fatal("failed to close fusemanager")
	}
}
