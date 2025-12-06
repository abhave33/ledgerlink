package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	ledgerpb "ledgerlink/proto/ledger"
)

// server implements the gRPC LedgerService.
type server struct {
	ledgerpb.UnimplementedLedgerServiceServer
	db *pgxpool.Pool
}

// ---------- helpers ----------

func toCents(m *ledgerpb.Money) int64 {
	if m == nil {
		return 0
	}
	return m.AmountCents
}

func fromCents(currency string, cents int64) *ledgerpb.Money {
	return &ledgerpb.Money{
		Currency:    currency,
		AmountCents: cents,
	}
}

// ensureAccount creates an account row if it doesn't exist.
func (s *server) ensureAccount(ctx context.Context, tx pgx.Tx, accountID string) error {
	var exists bool
	err := tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1)",
		accountID,
	).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		_, err = tx.Exec(ctx,
			"INSERT INTO accounts (id, balance_cents, held_cents) VALUES ($1, 0, 0)",
			accountID,
		)
	}
	return err
}

// ---------- RPC implementations ----------

func (s *server) Authorize(ctx context.Context, req *ledgerpb.AuthorizationRequest) (*ledgerpb.AuthorizationResponse, error) {
	amount := toCents(req.Amount)
	if amount <= 0 {
		return &ledgerpb.AuthorizationResponse{
			Approved: false,
			Reason:   "invalid amount",
		}, nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if err := s.ensureAccount(ctx, tx, req.AccountId); err != nil {
		return nil, err
	}

	var balance, held int64
	err = tx.QueryRow(ctx,
		"SELECT balance_cents, held_cents FROM accounts WHERE id = $1 FOR UPDATE",
		req.AccountId,
	).Scan(&balance, &held)
	if err != nil {
		return nil, err
	}

	available := balance - held
	approved := available >= amount
	eventType := "DECLINED"
	reason := "insufficient funds"

	if approved {
		held += amount
		_, err = tx.Exec(ctx,
			"UPDATE accounts SET held_cents = $1 WHERE id = $2",
			held, req.AccountId,
		)
		if err != nil {
			return nil, err
		}
		eventType = "APPROVED"
		reason = "approved"
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ledger_events (transaction_id, account_id, stage, amount_cents, event_type)
         VALUES ($1, $2, $3, $4, $5)`,
		req.TransactionId, req.AccountId, "AUTH", amount, eventType,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &ledgerpb.AuthorizationResponse{
		Approved: approved,
		Reason:   reason,
	}, nil
}

func (s *server) Capture(ctx context.Context, req *ledgerpb.CaptureRequest) (*ledgerpb.CaptureResponse, error) {
	amount := toCents(req.Amount)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if err := s.ensureAccount(ctx, tx, req.AccountId); err != nil {
		return nil, err
	}

	var balance, held int64
	err = tx.QueryRow(ctx,
		"SELECT balance_cents, held_cents FROM accounts WHERE id = $1 FOR UPDATE",
		req.AccountId,
	).Scan(&balance, &held)
	if err != nil {
		return nil, err
	}

	if held < amount {
		return &ledgerpb.CaptureResponse{
			Success: false,
			Reason:  "not enough held funds",
		}, nil
	}

	held -= amount
	balance -= amount

	_, err = tx.Exec(ctx,
		"UPDATE accounts SET balance_cents = $1, held_cents = $2 WHERE id = $3",
		balance, held, req.AccountId,
	)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ledger_events (transaction_id, account_id, stage, amount_cents, event_type)
         VALUES ($1, $2, $3, $4, $5)`,
		req.TransactionId, req.AccountId, "CAPTURE", amount, "COMPLETED",
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &ledgerpb.CaptureResponse{
		Success: true,
		Reason:  "captured",
	}, nil
}

func (s *server) Settle(ctx context.Context, req *ledgerpb.SettlementRequest) (*ledgerpb.SettlementResponse, error) {
	amount := toCents(req.Amount)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if err := s.ensureAccount(ctx, tx, req.AccountId); err != nil {
		return nil, err
	}

	// For demo: record settlement as an event only.
	_, err = tx.Exec(ctx,
		`INSERT INTO ledger_events (transaction_id, account_id, stage, amount_cents, event_type)
         VALUES ($1, $2, $3, $4, $5)`,
		req.TransactionId, req.AccountId, "SETTLE", amount, "SETTLED",
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &ledgerpb.SettlementResponse{
		Success: true,
		Reason:  "settled",
	}, nil
}

func (s *server) GetAccount(ctx context.Context, req *ledgerpb.GetAccountRequest) (*ledgerpb.GetAccountResponse, error) {
	var balance, held int64
	err := s.db.QueryRow(ctx,
		"SELECT balance_cents, held_cents FROM accounts WHERE id = $1",
		req.AccountId,
	).Scan(&balance, &held)
	if err != nil {
		return nil, err
	}

	return &ledgerpb.GetAccountResponse{
		AccountId: req.AccountId,
		Balance:   fromCents("USD", balance),
		Held:      fromCents("USD", held),
	}, nil
}

// ---------- Kafka consumer (idempotent) ----------

type KafkaTransactionMessage struct {
	MessageID     string `json:"message_id"`
	Stage         string `json:"stage"` // AUTH, CAPTURE, SETTLE
	TransactionID string `json:"transaction_id"`
	AccountID     string `json:"account_id"`
	AmountCents   int64  `json:"amount_cents"`
}

func (s *server) startKafkaConsumer(ctx context.Context, brokers []string, topic, groupID string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	log.Printf("Kafka consumer started on topic=%s group=%s", topic, groupID)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("kafka read error: %v", err)
			return
		}

		var msg KafkaTransactionMessage
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("invalid message: %v", err)
			continue
		}

		if err := s.processKafkaMessage(ctx, &msg); err != nil {
			log.Printf("error processing message %s: %v", msg.MessageID, err)
		}
	}
}

func (s *server) processKafkaMessage(ctx context.Context, msg *KafkaTransactionMessage) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Idempotency: skip if already processed
	var exists bool
	err = tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM processed_messages WHERE message_id = $1)",
		msg.MessageID,
	).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// Basic routing by stage (for demo, we just log)
	log.Printf("Processing Kafka message: stage=%s tx=%s acct=%s amount=%d",
		msg.Stage, msg.TransactionID, msg.AccountID, msg.AmountCents)

	_, err = tx.Exec(ctx,
		"INSERT INTO processed_messages (message_id) VALUES ($1)",
		msg.MessageID,
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ---------- main ----------

func main() {
	ctx := context.Background()

	dbURL := os.Getenv("LEDGERLINK_DB_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/ledgerlink?sslmode=disable"
	}

	dbpool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("unable to connect to db: %v", err)
	}
	defer dbpool.Close()

	s := &server{db: dbpool}

	// Optional Kafka consumer
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers != "" {
		go s.startKafkaConsumer(ctx,
			[]string{kafkaBrokers}, // e.g. "localhost:9092"
			"ledger-transactions",
			"ledgerlink-consumer",
		)
	} else {
		log.Println("KAFKA_BROKERS not set; Kafka consumer disabled")
	}

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	ledgerpb.RegisterLedgerServiceServer(grpcServer, s)
	reflection.Register(grpcServer)

	log.Printf("LedgerLink gRPC server listening on :%s", grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	// to avoid unused imports complaints if you tweak things
	_ = fmt.Sprintf
}
