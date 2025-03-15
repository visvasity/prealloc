// Copyright (c) 2025 Visvasity LLC

package prealloc

import (
	"context"
	"log"
	"os"
	"slices"
	"sync"

	"github.com/visvasity/storage"
)

type Allocator struct {
	lifeCtx    context.Context
	lifeCancel context.CancelCauseFunc

	wg sync.WaitGroup

	size int

	object *storage.Object

	requestCh chan chan *storage.Block

	blocks []*storage.Block
}

// New creates a pre-allocator instance. Preallocators keep a cache of
// temporary blocks for quick use without incurring the cost of allocation.
func New(ctx context.Context, object *storage.Object) (*Allocator, error) {
	size := 32 // TODO: Choose this value dynamically.
	blocks, err := object.AllocBlocks(ctx, size)
	if err != nil {
		return nil, err
	}

	lctx, lcancel := context.WithCancelCause(context.Background())

	v := &Allocator{
		lifeCtx:    lctx,
		lifeCancel: lcancel,
		object:     object,
		size:       size,
		requestCh:  make(chan chan *storage.Block),

		blocks: blocks,
	}

	v.wg.Add(1)
	go v.goPrealloc()

	return v, nil
}

// Close releases preallocated blocks and shuts down the allocator.
func (v *Allocator) Close() error {
	v.lifeCancel(os.ErrClosed)
	v.wg.Wait()

	// Free all preallocated blocks.
	cas := make([]*storage.CompareAndUpdateItem, len(v.blocks))
	for _, b := range v.blocks {
		cas = append(cas, storage.CompareAndDeleteBlockItem(b))
	}
	if err := v.object.CompareAndUpdate(context.Background(), cas...); err != nil {
		log.Printf("could not free %d temporary blocks (ignored): %v", len(v.blocks), err)
	}

	storage.ReleaseBlocks(v.blocks)
	v.blocks = nil
	return nil
}

// Get returns a preallocated temporary block. Returns nil if the allocator is
// closed.
func (v *Allocator) Get() *storage.Block {
	replyCh := make(chan *storage.Block)
	select {
	case <-v.lifeCtx.Done():
		return nil
	case v.requestCh <- replyCh:
	}

	select {
	case <-v.lifeCtx.Done():
		return nil
	case b := <-replyCh:
		return b
	}
}

func (v *Allocator) goPrealloc() {
	defer v.wg.Done()

	readyCh := make(chan struct{})
	close(readyCh)

	neverCh := make(chan struct{})
	defer close(neverCh)

	for {
		allocCh := neverCh
		nblocks := len(v.blocks)
		if nblocks < v.size {
			allocCh = readyCh
		}

		requestCh := v.requestCh
		if len(v.blocks) == 0 {
			requestCh = make(chan chan *storage.Block)
		}

		select {
		case <-v.lifeCtx.Done():
			return

		case <-allocCh:
			n := v.size - len(v.blocks)
			bs, err := v.object.AllocBlocks(v.lifeCtx, n)
			if err != nil {
				log.Printf("could not allocate %d blocks (stopped): %v", n, err)
				return
			}
			v.blocks = append(v.blocks, bs...)

		case replyCh := <-requestCh:
			select {
			case replyCh <- v.blocks[0]:
			case <-v.lifeCtx.Done():
				return
			}
			v.blocks = slices.Delete(v.blocks, 0, 1)
		}
	}
}

func (v *Allocator) lbas() []storage.LBA {
	vs := make([]storage.LBA, len(v.blocks))
	for i, b := range v.blocks {
		vs[i] = b.LBA()
	}
	return vs
}
