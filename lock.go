package mongodb_lock

import (
	"context"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultUniqueField = "key"
	DefaultTTLField    = "created_at"
)

var (
	ErrLockIndexCouldNotEnsure = errors.New("could not ensure that the unique index or ttl index exist")
	ErrLockIndexCouldNotCreate = errors.New("could not create indices on either the unique or ttl field(s)")
	ErrParamsIsNil             = errors.New("params cannot be nil")
	ErrDuplicateKey            = errors.New("duplicate key error")
	ErrNotFoundKey             = errors.New("key does not exist")
	ErrCouldNotUnlock          = errors.New("could not unlock")
)

func ensureMongoDBLockIndex(ctx context.Context, c *mongo.Collection, expireAfter time.Duration) error {
	if c == nil {
		return ErrParamsIsNil
	}

	indexView := c.Indexes()
	indexes, err := indexView.ListSpecifications(ctx)
	if err != nil {
		return errors.Wrap(err, ErrLockIndexCouldNotEnsure.Error())
	}

	ttlIndexExists := false
	uniqueIndexExists := false

	for _, index := range indexes {
		elements, err := index.KeysDocument.Elements()
		if err != nil {
			return errors.Wrap(err, ErrLockIndexCouldNotEnsure.Error())
		}

		if len(elements) == 1 {
			if elements[0].String() == DefaultUniqueField && index.Unique != nil && *index.Unique {
				uniqueIndexExists = true
			}

			if elements[0].String() == DefaultTTLField && index.ExpireAfterSeconds != nil && expireAfter.Seconds() == float64(*index.ExpireAfterSeconds) {
				ttlIndexExists = true
			}
		}
	}

	if !uniqueIndexExists {
		if _, err := indexView.CreateOne(
			ctx,
			mongo.IndexModel{
				Keys:    bson.D{{Key: DefaultUniqueField, Value: 1}},
				Options: options.Index().SetUnique(true),
			},
		); err != nil {
			return errors.Wrap(err, ErrLockIndexCouldNotCreate.Error())
		}
	}

	if !ttlIndexExists {
		if _, err := indexView.CreateOne(
			ctx,
			mongo.IndexModel{
				Keys:    bson.D{{Key: DefaultTTLField, Value: 1}},
				Options: options.Index().SetExpireAfterSeconds(int32(expireAfter.Seconds())),
			},
		); err != nil {
			return errors.Wrap(err, ErrLockIndexCouldNotCreate.Error())
		}
	}

	return nil
}

type MongoDBLockerOptions struct {
	ExpiresAfter time.Duration
}

type MongoDBLockerOption func(*MongoDBLockerOptions)

func WithMongoDBLockerExpireAfter(expireAfter time.Duration) MongoDBLockerOption {
	return func(opts *MongoDBLockerOptions) {
		opts.ExpiresAfter = expireAfter
	}
}

// A mongoDBLocker is a mutual exclusion lock.
// This mutex is distributed and backed by mongodb.
type mongoDBLocker struct {
	opts MongoDBLockerOptions
	c    *mongo.Collection
}

// NewMongoDBLockerAlways creates a new mongodb-backed distributed locker.
func NewMongoDBLocker(ctx context.Context, c *mongo.Collection, opts ...MongoDBLockerOption) (*mongoDBLocker, error) {
	if err := c.Database().Client().Ping(ctx, nil); err != nil {
		return nil, errors.Wrapf(err, gocron.ErrFailedToConnectToRedis.Error())
	}

	return newMongoDBLocker(ctx, c, opts...)
}

// NewMongoDBLockerAlways creates a new mongodb-backed distributed locker, even when pinging fails.
func NewMongoDBLockerAlways(ctx context.Context, c *mongo.Collection, opts ...MongoDBLockerOption) (*mongoDBLocker, error) {
	return newMongoDBLocker(ctx, c, opts...)
}

func newMongoDBLocker(ctx context.Context, c *mongo.Collection, opts ...MongoDBLockerOption) (*mongoDBLocker, error) {
	if c == nil {
		return nil, ErrParamsIsNil
	}

	m := &mongoDBLocker{c: c}
	for _, opt := range opts {
		opt(&m.opts)
	}

	if err := ensureMongoDBLockIndex(ctx, c, m.opts.ExpiresAfter); err != nil {
		return nil, err
	}

	return m, nil
}

var (
	_ gocron.Lock   = (*mongoDBLock)(nil)
	_ gocron.Locker = (*mongoDBLocker)(nil)
)

type mongoDBLock struct {
	key string
	c   *mongo.Collection
}

func (ml *mongoDBLocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	_, err := ml.c.InsertOne(ctx, bson.M{
		DefaultUniqueField: key,
		DefaultTTLField:    time.Now(),
	})

	if err != nil && mongo.IsDuplicateKeyError(err) {
		return nil, ErrDuplicateKey
	}

	return &mongoDBLock{
		key: key,
		c:   ml.c,
	}, nil
}

func (ml *mongoDBLock) Unlock(ctx context.Context) error {
	res, err := ml.c.DeleteOne(ctx, bson.M{"key": ml.key})

	if err != nil {
		return ErrCouldNotUnlock
	} else if res.DeletedCount == 0 {
		return ErrNotFoundKey
	}

	return nil
}
