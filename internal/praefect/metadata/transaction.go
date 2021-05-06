package metadata

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"google.golang.org/grpc/metadata"
)

const (
	// TransactionMetadataKey is the key used to store transaction
	// information in the gRPC metadata.
	TransactionMetadataKey = "gitaly-reference-transaction"
)

var (
	// ErrTransactionNotFound indicates the transaction metadata could not
	// be found
	ErrTransactionNotFound = errors.New("transaction not found")
)

// Transaction stores parameters required to identify a reference
// transaction.
type Transaction struct {
	// BackchannelID is the ID of the backchannel that corresponds to the Praefect
	// that is handling the transaction. This field is filled in by the Gitaly.
	BackchannelID backchannel.ID `json:"backchannel_id,omitempty"`
	// ID is the unique identifier of a transaction
	ID uint64 `json:"id"`
	// Node is the name used to cast a vote
	Node string `json:"node"`
	// Primary identifies the node's role in this transaction
	Primary bool `json:"primary"`
}

// serialize serializes a `Transaction` into a string.
func (t Transaction) serialize() (string, error) {
	marshalled, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(marshalled), nil
}

// transactionFromSerialized creates a transaction from a `serialize()`d string.
func transactionFromSerialized(serialized string) (Transaction, error) {
	decoded, err := base64.StdEncoding.DecodeString(serialized)
	if err != nil {
		return Transaction{}, err
	}

	var tx Transaction
	if err := json.Unmarshal(decoded, &tx); err != nil {
		return Transaction{}, err
	}

	return tx, nil
}

// InjectTransaction injects reference transaction metadata into an incoming context
func InjectTransaction(ctx context.Context, tranasctionID uint64, node string, primary bool) (context.Context, error) {
	transaction := Transaction{
		ID:      tranasctionID,
		Node:    node,
		Primary: primary,
	}

	serialized, err := transaction.serialize()
	if err != nil {
		return nil, err
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	} else {
		md = md.Copy()
	}
	md.Set(TransactionMetadataKey, serialized)

	return metadata.NewIncomingContext(ctx, md), nil
}

// TransactionFromContext extracts `Transaction` from an incoming context. In
// case the metadata key is not set, the function will return
// `ErrTransactionNotFound`.
func TransactionFromContext(ctx context.Context) (Transaction, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Transaction{}, ErrTransactionNotFound
	}

	serialized := md[TransactionMetadataKey]
	if len(serialized) == 0 {
		return Transaction{}, ErrTransactionNotFound
	}

	transaction, err := transactionFromSerialized(serialized[0])
	if err != nil {
		return Transaction{}, fmt.Errorf("from serialized: %w", err)
	}

	// For backwards compatibility during an upgrade, we still need to accept transactions
	// from non-multiplexed connections. From 14.0 onwards, we can expect every transaction to
	// originate from a multiplexed connection and should drop the error check below.
	transaction.BackchannelID, err = backchannel.GetPeerID(ctx)
	if err != nil && !errors.Is(err, backchannel.ErrNonMultiplexedConnection) {
		return Transaction{}, fmt.Errorf("get peer id: %w", err)
	}

	return transaction, nil
}

// TransactionMetadataFromContext extracts transaction-related metadata from
// the given context. No error is returned in case no transaction was found.
func TransactionMetadataFromContext(ctx context.Context) (*Transaction, *PraefectServer, error) {
	transaction, err := TransactionFromContext(ctx)
	if err != nil {
		if err != ErrTransactionNotFound {
			return nil, nil, err
		}
		return nil, nil, nil
	}

	praefect, err := PraefectFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	return &transaction, praefect, nil
}
