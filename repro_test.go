package etcd_deadline_repro

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcd "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func generateLockID() string {
	var lockID [32]byte
	rng.Read(lockID[:])
	return string(lockID[:])
}

func requireNoError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
}

func TestLockTimeout(t *testing.T) {
	etcdClient, err := etcd.New(etcd.Config{Endpoints: []string{"localhost:2379"}})
	requireNoError(t, err)

	var randomPrefix [16]byte
	rng.Read(randomPrefix[:])
	randomPrefixStr := fmt.Sprintf("%x", randomPrefix)

	etcdLease := etcd.NewLease(etcdClient)
	etcdKV := etcd.NewKV(etcdClient)
	for iii := 0; iii < 1_000_000; iii++ {
		t.Run(fmt.Sprintf("%d", iii), func(t *testing.T) {
			prefix := fmt.Sprintf("%s_%d", randomPrefixStr, iii)

			lease1, err := etcdLease.Grant(context.Background(), 10000)
			requireNoError(t, err)

			lease2, err := etcdLease.Grant(context.Background(), 10000)
			requireNoError(t, err)

			lockID := generateLockID()
			cmp1 := etcd.Compare(etcd.CreateRevision(prefix), "=", 0)
			put1 := etcd.OpPut(prefix, lockID, etcd.WithLease(lease1.ID))

			resp1, err := etcdKV.Txn(context.Background()).If(cmp1).Then(put1).Commit()
			requireNoError(t, err)
			if !resp1.Succeeded {
				t.Fatalf("resp1.Succeeded is expected to be true")
			}

			for i := 0; i < 100; i++ {
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.Intn(1*int(time.Millisecond))))
					defer cancel()

					cmp2 := etcd.Compare(etcd.CreateRevision(prefix), "=", 0)
					put2 := etcd.OpPut(prefix, lockID, etcd.WithLease(lease2.ID))
					_, err := etcdKV.Txn(ctx).If(cmp2).Then(put2).Commit()
					if err != nil && !isTimeoutError(err) {
						t.Fatalf("expected timeout, but got %T: %+v", err, err)
					}
				}()
			}
		})

		if t.Failed() {
			return
		}
	}
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, rpctypes.ErrTimeout) {
		return true
	}

	var holder interface{ GRPCStatus() *status.Status }
	if errors.As(err, &holder) {
		s := holder.GRPCStatus()
		if s.Code() == codes.DeadlineExceeded {
			return true
		}
	}

	return false
}
