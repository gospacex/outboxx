// outbox 补发器的可执行判据：多副本不重复投递（行级认领）、崩溃后由租约自愈、
// 失败重试与放弃阈值、空 payload 直接放弃。
//
// 为什么值得单独一个测：这段逻辑的正确性只在**多副本 / 崩溃**这两类场景下才被
// 检验到，而它恰恰是「单副本演示」跑不出来、上线后才发现的那一类缺陷。判据必须
// 能在本机把两个 Runner 摆在同一张表上，所以用 sqlite 内存库 + 假 producer。
// 需要 CGO_ENABLED=1（mattn/go-sqlite3）。
package outboxx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gospacex/mqx"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// fakeProducer 记录投递过的消息（key = outbox 行 id）。
type fakeProducer struct {
	mu   sync.Mutex
	sent []string
	// sendFn 带 ctx：投递是否尊重 deadline 是「退出链有没有界」的关键，
	// 拿不到 ctx 就构造不出「producer 不理 ctx」这一类真实故障。
	sendFn func(ctx context.Context, msg *mqx.Message) error
	// entered 可选：每次 Send 进入时投递一个信号（非阻塞，缓冲满即丢）。
	// 「等这一轮真的进入投递」必须有它 —— 否则 Stop 可能在 flush 开始之前
	// 就返回，测出来的是空转路径，卡死的那条根本没被走过。
	entered chan struct{}
}

func (p *fakeProducer) Send(ctx context.Context, msg *mqx.Message) error {
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}
	// 注意：sendFn 在锁外调用。「等投递开始」与「投递阻塞中」两个测试都依赖
	// 它在锁外 —— 持锁阻塞会让同进程的其它用例一起挂住。
	if p.sendFn != nil {
		if err := p.sendFn(ctx, msg); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, msg.Key)
	return nil
}
func (p *fakeProducer) SendBatch(context.Context, []*mqx.Message) error { return nil }
func (p *fakeProducer) Close() error                                    { return nil }
func (p *fakeProducer) Name() string                                    { return "fake" }
func (p *fakeProducer) HealthCheck() bool                               { return true }

func (p *fakeProducer) sentKeys() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sent...)
}

// newTestDB 建 sqlite 内存库并建 outbox 表。
//
// 用 file::memory:?cache=shared 的**单连接**库：两个 Runner 必须真的看到同一张表，
// 各自一个 :memory: 库的话「重复投递」根本构造不出来，测出来的绿是假的。
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:outbox-test?mode=memory&cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// 连接池限 1：sqlite 内存库是「每连接一份」，多连接会看到不同的库。
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&Event{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	// 表级清空：多个用例共享同一个内存库（cache=shared 是进程级的）。
	if err := db.Exec("DELETE FROM outbox").Error; err != nil {
		t.Fatalf("clean outbox: %v", err)
	}
	return db
}

func seed(t *testing.T, db *gorm.DB, payload string) uint64 {
	t.Helper()
	ev := Event{Topic: "order.completed.v1", EventType: "order.completed",
		Payload: payload, Status: StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := db.Create(&ev).Error; err != nil {
		t.Fatalf("seed event: %v", err)
	}
	return ev.ID
}

func statusOf(t *testing.T, db *gorm.DB, id uint64) (int, int) {
	t.Helper()
	var ev Event
	if err := db.First(&ev, id).Error; err != nil {
		t.Fatalf("read event %d: %v", id, err)
	}
	return ev.Status, ev.RetryCount
}

// 主路径：一行事件被投递一次，状态推进到 sent。
func TestFlushOnceSendsPendingEvent(t *testing.T) {
	db := newTestDB(t)
	id := seed(t, db, `{"order_id":1}`)
	p := &fakeProducer{}
	r := NewRunner(db, p, nil, Config{})

	if ab := r.FlushOnce(); len(ab) != 0 {
		t.Fatalf("不应有放弃项，实际 %v", ab)
	}
	if got := p.sentKeys(); len(got) != 1 || got[0] != "1" {
		t.Fatalf("投递记录 = %v, want [1]", got)
	}
	if st, _ := statusOf(t, db, id); st != StatusSent {
		t.Fatalf("status = %d, want %d", st, StatusSent)
	}
	// 第二轮不该再发同一行（认领 + sent 守卫）。
	p.sent = nil
	r.FlushOnce()
	if got := p.sentKeys(); len(got) != 0 {
		t.Fatalf("已发送的行不得重投，实际 %v", got)
	}
}

// 行级认领：一行只能被一个 Runner 投递 —— 两个 Runner 处理同一批 pending 时，
// 总投递次数必须等于行数（而不是 2×行数）。
//
// 反向探针：把 claim 改成「先 SELECT 判断 status」或直接跳过认领，本用例立刻红。
func TestRowLevelClaimPreventsDoubleSend(t *testing.T) {
	db := newTestDB(t)
	for i := 0; i < 5; i++ {
		seed(t, db, `{"order_id":1}`)
	}
	p := &fakeProducer{}
	r1 := NewRunner(db, p, nil, Config{})
	r2 := NewRunner(db, p, nil, Config{})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r1.FlushOnce() }()
	go func() { defer wg.Done(); r2.FlushOnce() }()
	wg.Wait()

	seen := map[string]int{}
	for _, k := range p.sentKeys() {
		seen[k]++
	}
	if len(seen) != 5 {
		t.Fatalf("应覆盖 5 行，实际覆盖 %d 行（keys=%v）", len(seen), seen)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("行 %s 被投递 %d 次（多副本重复投递）", k, n)
		}
	}
}

// 崩溃自愈：认领后进程死掉（状态停在 claimed、没有后续推进），
// 租约过期后另一副本必须能重新认领并投递 —— 否则那一行会永久卡住。
func TestStaleClaimIsReclaimedAfterLease(t *testing.T) {
	db := newTestDB(t)
	id := seed(t, db, `{"order_id":1}`)
	// 模拟「另一个副本认领后崩了」：直接把状态置为 claimed，updated_at 设成很久以前。
	if err := db.Model(&Event{}).Where("id = ?", id).
		Updates(map[string]any{"status": StatusClaimed, "updated_at": time.Now().Add(-2 * time.Hour)}).Error; err != nil {
		t.Fatalf("simulate stale claim: %v", err)
	}

	p := &fakeProducer{}
	r := NewRunner(db, p, nil, Config{ClaimLease: time.Minute})
	r.FlushOnce()

	if got := p.sentKeys(); len(got) != 1 {
		t.Fatalf("租约过期后应被重新认领并投递，实际 %v", got)
	}
	if st, _ := statusOf(t, db, id); st != StatusSent {
		t.Fatalf("status = %d, want %d", st, StatusSent)
	}
}

// 租约**未**过期时不许抢：另一个副本正在处理的行走不到自己手上。
func TestFreshClaimIsNotStolen(t *testing.T) {
	db := newTestDB(t)
	id := seed(t, db, `{"order_id":1}`)
	if err := db.Model(&Event{}).Where("id = ?", id).
		Update("status", StatusClaimed).Error; err != nil {
		t.Fatalf("simulate in-flight claim: %v", err)
	}

	p := &fakeProducer{}
	r := NewRunner(db, p, nil, Config{ClaimLease: time.Minute})
	r.FlushOnce()

	if got := p.sentKeys(); len(got) != 0 {
		t.Fatalf("在飞行不得被抢走，实际投递 %v", got)
	}
	if st, _ := statusOf(t, db, id); st != StatusClaimed {
		t.Fatalf("status = %d, want %d", st, StatusClaimed)
	}
}

// 投递失败 → 退回 pending 且 retry_count+1（下轮可重试，不必等租约）。
func TestSendFailureReturnsToPendingAndCountsRetry(t *testing.T) {
	db := newTestDB(t)
	id := seed(t, db, `{"order_id":1}`)
	p := &fakeProducer{sendFn: func(context.Context, *mqx.Message) error { return errors.New("broker down") }}
	r := NewRunner(db, p, nil, Config{})

	r.FlushOnce()

	st, rc := statusOf(t, db, id)
	if st != StatusPending {
		t.Fatalf("失败后应退回 pending（否则要等租约才重试），实际 status=%d", st)
	}
	if rc != 1 {
		t.Fatalf("retry_count = %d, want 1", rc)
	}
}

// 重试到上限 → 放弃（status=abandon），不再无限重投。
func TestRetryCapAbandons(t *testing.T) {
	db := newTestDB(t)
	id := seed(t, db, `{"order_id":1}`)
	p := &fakeProducer{sendFn: func(context.Context, *mqx.Message) error { return errors.New("broker down") }}
	r := NewRunner(db, p, nil, Config{RetryCap: 3})

	for i := 0; i < 3; i++ {
		r.FlushOnce()
	}

	st, _ := statusOf(t, db, id)
	if st != StatusAbandon {
		t.Fatalf("达到重试上限后应放弃，实际 status=%d", st)
	}
}

// 空 payload（上游 marshal 失败）直接放弃，且**不占用**重试配额。
func TestEmptyPayloadAbandonedWithoutRetryQuota(t *testing.T) {
	db := newTestDB(t)
	id := seed(t, db, "")
	p := &fakeProducer{}
	r := NewRunner(db, p, nil, Config{})

	r.FlushOnce()

	if got := p.sentKeys(); len(got) != 0 {
		t.Fatalf("空 payload 不得投递，实际 %v", got)
	}
	st, rc := statusOf(t, db, id)
	if st != StatusAbandon {
		t.Fatalf("空 payload 应直接放弃，实际 status=%d", st)
	}
	if rc != 0 {
		t.Fatalf("空 payload 不该占用重试配额，实际 retry_count=%d", rc)
	}
}

// 单次投递有 deadline：producer 卡住时 sendOne 必须按时返回（而不是无限等待），
// 行退回 pending 等下一轮重试。
//
// 这条的必要性来自一次真机事故形态：producer.Send 用 `context.Background()`
// 起手，broker 不可达时永不返回 —— 退出链上没有 deadline，`Stop()` 就会一直
// 卡在 `<-r.done`，进程退不掉，K8s 只能 SIGKILL。
//
// 反向探针：把 sendOne 的 WithTimeout 换回 context.Background()，本用例会一直
// 挂到 go test 超时（不是「红」，是「不返回」—— 正是被修掉的那种失效）。
func TestSendTimeoutBoundsOneDelivery(t *testing.T) {
	db := newTestDB(t)
	id := seed(t, db, `{"order_id":1}`)
	p := &fakeProducer{sendFn: func(ctx context.Context, _ *mqx.Message) error {
		// 尊重 ctx 的 producer：卡到 deadline 就返回。
		<-ctx.Done()
		return ctx.Err()
	}}
	r := NewRunner(db, p, nil, Config{SendTimeout: 100 * time.Millisecond})

	done := make(chan struct{})
	go func() { defer close(done); r.FlushOnce() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("FlushOnce 未在 3s 内返回：单次投递没有 deadline，退出链无界")
	}

	st, rc := statusOf(t, db, id)
	if st != StatusPending {
		t.Fatalf("超时后应退回 pending 等下轮重试，实际 status=%d", st)
	}
	if rc != 1 {
		t.Fatalf("retry_count = %d, want 1", rc)
	}
}

// Stop 有界：**不理 ctx** 的 producer（kafka-go joinGroup 就是这一类）卡住时，
// Stop 必须按 StopGrace 放弃等待并返回 —— 宁可丢下在飞的那一轮（行停在
// claimed，等租约过期自愈），也不能让进程退不掉。
//
// 反向探针：把 Stop 的 `select <-time.After(grace)` 换回裸 `<-r.done`，
// 本用例会在 2s 处 Fatal（旧实现要等满 producer 的 30s）。
func TestStopReturnsWithinGraceWhenProducerHangs(t *testing.T) {
	db := newTestDB(t)
	seed(t, db, `{"order_id":1}`)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	p := &fakeProducer{
		entered: entered,
		sendFn: func(context.Context, *mqx.Message) error {
			<-release // 完全不理 ctx：真实 broker 客户端有相当一部分是这个德性
			return nil
		},
	}
	r := NewRunner(db, p, nil, Config{
		PollInterval: 10 * time.Millisecond,
		SendTimeout:  100 * time.Millisecond,
		StopGrace:    200 * time.Millisecond,
	})
	r.Start()
	t.Cleanup(func() { close(release) })

	// 等一轮真的进入投递：在 flush 开始前 Stop 走的是空转路径，测不到东西。
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("投递未在 3s 内开始，用例前提不成立")
	}

	start := time.Now()
	r.Stop()
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("Stop 耗时 %v，超过 StopGrace(200ms) 一个数量级：退出链无界", elapsed)
	}
}

// producer 为 nil（MQ 未装配 / 降级）时不启动，且必须留 Error 痕迹。
//
// 「静默不启动」是这类组件最危险的形态：业务还在往表里写 status=0 的事件，
// 没有任何投递者，表会无声膨胀到爆。
func TestNilProducerDoesNotStart(t *testing.T) {
	db := newTestDB(t)
	seed(t, db, `{"order_id":1}`)
	r := NewRunner(db, nil, nil, Config{})

	r.Start()
	r.FlushOnce() // 即便被直接调用也应安全（无投递者）

	if st, _ := statusOf(t, db, 1); st != StatusPending {
		t.Fatalf("无投递者时状态不该被推进，实际 status=%d", st)
	}
}
