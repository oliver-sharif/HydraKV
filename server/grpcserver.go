package server

import (
	"context"
	"hydrakv/utils"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"hydrakv/envhandler"
	"hydrakv/server/hydrakv/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// =========================
// gRPC Interceptors
// =========================

// Global request limit (concurrency)
func grpcRequestLimitInterceptor(limit int) grpc.UnaryServerInterceptor {
	sem := make(chan struct{}, limit)

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {

		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			return handler(ctx, req)
		default:
			return nil, status.Error(
				codes.ResourceExhausted,
				"grpc request limit reached",
			)
		}
	}
}

// Require a deadline and cap its maximum duration
func grpcDeadlineInterceptor() grpc.UnaryServerInterceptor {
	MaxDuration := time.Duration(*envhandler.ENV.GRPC_MAX_DURATION) * time.Second
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {

		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, status.Error(
				codes.InvalidArgument,
				"grpc deadline required",
			)
		}

		if time.Until(deadline) > MaxDuration {
			return nil, status.Error(
				codes.InvalidArgument,
				"grpc deadline too long",
			)
		}

		return handler(ctx, req)
	}
}

// =========================
// KVService
// =========================

type KVService struct {
	kv kvLogic
	kvpb.UnimplementedKVServiceServer
}

// =========================
// GRPCServer
// =========================

type GRPCServer struct {
	server *grpc.Server
	lis    net.Listener
	ks     *KVService
}

// NewGRPCServer creates a new gRPC server instance
func NewGRPCServer(svc kvLogic) *GRPCServer {
	return &GRPCServer{
		ks: &KVService{kv: svc},
	}
}

// Start starts the gRPC server
func (g *GRPCServer) Start(ip string, port int) {
	var err error

	g.lis, err = net.Listen("tcp", ip+":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	concurrentStreams := *envhandler.ENV.GRPC_MAX_CONCURRENT_STREAMS
	reqLimit := *envhandler.ENV.GRPC_REQ_LIMIT

	g.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<20), // 1 MB
		grpc.MaxSendMsgSize(1<<20), // 1 MB
		grpc.MaxConcurrentStreams(uint32(concurrentStreams)),
		grpc.ChainUnaryInterceptor(
			grpcRequestLimitInterceptor(reqLimit),
			grpcDeadlineInterceptor(),
		),
	)

	kvpb.RegisterKVServiceServer(g.server, g.ks)

	log.Printf("Starting GRPCServer on %s:%d\n", ip, port)
	if err := g.server.Serve(g.lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Stop stops the gRPC server gracefully
func (g *GRPCServer) Stop() {
	if g.server != nil {
		g.server.GracefulStop()
		log.Println("GRPCServer stopped")
	}
	if g.lis != nil {
		_ = g.lis.Close()
		log.Println("GRPCServer listener closed")
	}
}

// =========================
// RPC Implementations
// =========================

func (s *KVService) CreateDB(ctx context.Context, req *kvpb.CreateDBRequest,
) (*kvpb.CreateDBResponse, error) {

	// bye bye
	if !utils.U.CheckDbName(req.Name) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid db name")
	}

	err, exists, created, apikey := s.kv.NewDB(req.Name)
	if err != nil {
		return nil, err
	}

	return &kvpb.CreateDBResponse{
		Name:    strings.ToUpper(req.Name),
		Created: created,
		Exists:  exists,
		Apikey:  apikey,
	}, nil
}

func (s *KVService) Set(
	ctx context.Context,
	req *kvpb.SetRequest,
) (*kvpb.OKResponse, error) {

	if !utils.U.CheckDbName(req.Db) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid db name")
	}

	// if apikey is enabled, check it
	if *envhandler.ENV.APIKEY_ENABLED && !utils.U.IsApiKeyValid(req.Db, req.Apikey) {
		return nil, status.Errorf(codes.Unauthenticated, "invalid apikey")
	}

	ok := s.kv.Set(req.Db, req.Key, req.Value, req.Ttl)
	return &kvpb.OKResponse{Ok: ok}, nil
}

func (s *KVService) SetNX(
	ctx context.Context,
	req *kvpb.SetRequest,
) (*kvpb.OKResponse, error) {

	if !utils.U.CheckDbName(req.Db) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid db name")
	}
	// if apikey is enabled, check it
	if *envhandler.ENV.APIKEY_ENABLED && !utils.U.IsApiKeyValid(req.Db, req.Apikey) {
		return nil, status.Errorf(codes.Unauthenticated, "invalid apikey")
	}
	ok := s.kv.SetNX(req.Db, req.Key, req.Value, req.Ttl)
	return &kvpb.OKResponse{Ok: ok}, nil
}

func (s *KVService) Incr(
	ctx context.Context,
	req *kvpb.IncrRequest,
) (*kvpb.OKResponse, error) {

	if !utils.U.CheckDbName(req.Db) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid db name")
	}
	// if apikey is enabled, check it
	if *envhandler.ENV.APIKEY_ENABLED && !utils.U.IsApiKeyValid(req.Db, req.Apikey) {
		return nil, status.Errorf(codes.Unauthenticated, "invalid apikey")
	}
	ok := s.kv.Incr(req.Db, req.Key, req.Amount)
	return &kvpb.OKResponse{Ok: ok}, nil
}

func (s *KVService) Get(
	ctx context.Context,
	req *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {

	if !utils.U.CheckDbName(req.Db) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid db name")
	}
	// if apikey is enabled, check it
	if *envhandler.ENV.APIKEY_ENABLED && !utils.U.IsApiKeyValid(req.Db, req.Apikey) {
		return nil, status.Errorf(codes.Unauthenticated, "invalid apikey")
	}

	found, val := s.kv.Get(req.Db, req.Key)
	return &kvpb.GetResponse{
		Found: found,
		Value: val,
	}, nil
}

func (s *KVService) Delete(
	ctx context.Context,
	req *kvpb.DeleteRequest,
) (*kvpb.OKResponse, error) {

	if !utils.U.CheckDbName(req.Db) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid db name")
	}
	// if apikey is enabled, check it
	if *envhandler.ENV.APIKEY_ENABLED && !utils.U.IsApiKeyValid(req.Db, req.Apikey) {
		return nil, status.Errorf(codes.Unauthenticated, "invalid apikey")
	}

	ok := s.kv.Del(req.Db, req.Key)
	return &kvpb.OKResponse{Ok: ok}, nil
}

func (s *KVService) Exists(
	ctx context.Context,
	req *kvpb.ExistsRequest,
) (*kvpb.ExistsResponse, error) {

	if !utils.U.CheckDbName(req.Db) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid db name")
	}
	ok := s.kv.DBExists(req.Db)
	return &kvpb.ExistsResponse{Exists: ok}, nil
}

func (s *KVService) FiFoLiFoDelete(
	ctx context.Context,
	req *kvpb.FiFoLiFoDeleteRequest,
) (*kvpb.OKResponse, error) {
	err := s.kv.DelFiFoLiFo(req.Name)
	if err != nil {
		return &kvpb.OKResponse{Ok: false}, status.Error(codes.NotFound, err.Error())
	}
	return &kvpb.OKResponse{Ok: true}, nil
}

func (s *KVService) FiFoLiFoPush(
	ctx context.Context,
	req *kvpb.FiFoLiFoPushRequest,
) (*kvpb.OKResponse, error) {
	ok, err := s.kv.PushEntryFiFoLiFo(req.Name, req.Value)
	if err != nil {
		return &kvpb.OKResponse{Ok: false}, status.Error(codes.Internal, err.Error())
	}
	return &kvpb.OKResponse{Ok: ok}, nil
}

func (s *KVService) FiFoLiFoFPop(
	ctx context.Context,
	req *kvpb.FiFoLiFoPopRequest,
) (*kvpb.FiFoLiFoPopResponse, error) {
	val, err := s.kv.PopEntryFiFo(req.Name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &kvpb.FiFoLiFoPopResponse{Value: val}, nil
}

func (s *KVService) FiFoLiFoLPop(
	ctx context.Context,
	req *kvpb.FiFoLiFoPopRequest,
) (*kvpb.FiFoLiFoPopResponse, error) {
	val, err := s.kv.PopEntryLiFo(req.Name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &kvpb.FiFoLiFoPopResponse{Value: val}, nil
}
