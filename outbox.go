// Package outboxx 事务发件箱（各示例与服务共用一份，2026-10-15 自
// launchx/examples/pkg/outbox 上提）。
//
// 为什么抽成共享包而不是每个服务抄一份：所有服务都要「本地事务提交后
// 可靠投递事件」，语义完全一致（pending → sent / retry → abandon）。抄多份的
// 代价不是代码量，而是**语义漂移** —— 重试上限、空 payload 处理、轮询 ctx
// 是否入链这三条只要有一处写歪，故障表现就是「某个域的事件静默丢失」，
// 最难排查。
//
// 上提动机与边界（2026-10-15）：它一半是「与业务行同事务的落库」（gorm），
// 一半是「MQ 的可靠投递」（mqx），是跨域粘合层 —— 但三个示例（mall /
// micro / mesh）共用同一份语义，继续放在 launchx/examples 树里等于把
// 共享件藏在示例目录里。上提为独立模块 `github.com/gospacex/outboxx` 后，
// 各模块直接按模块依赖取用，不再依赖 launchx 的 examples 目录。
//
// 依赖取向：只引 gorm / mqx / logx / dbx / otelx，**不引 launchx**（旧实现里
// 续链用的 `launchx.Extract` 已换为 otelx/propagation 同名入口，本包 re-export
// 的 Extract 只是方便调用方少引一个包）。反向依赖 launchx 会形成
// launchx(库) ↔ outboxx(工具) 的环，故刻意切断。
//
// 落库侧用法（业务事务内，与业务行同事务）：
//
//	tx.Create(&outboxx.Event{Topic: ..., EventType: ..., Payload: ..., Traceparent: ...})
//
// 补发侧用法（进程内单 goroutine）：
//
//	runner := outboxx.NewRunner(db, app.MQ(), app.Logger(), outboxx.Config{TraceOn: true})
//	runner.Start()
//	defer runner.Stop()
//
// 多副本：补发器是**行级认领**的（pending → claimed → sent/abandon，见 Runner.claim），
// 因此同一张表上跑多个 Runner 不会重复投递同一行；崩溃的 Runner 留下的 claimed
// 行由租约（Config.ClaimLease）兜底自愈。投递本身仍是**至少一次**语义（认领租约
// 内慢发送、或发送成功但 markSent 前崩溃，都会导致重投），消费侧必须幂等 ——
// mesh 的 points 域用 `points_log.order_id` 唯一索引做到这一点。
package outboxx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gospacex/dbx/orm"
	otelxprop "github.com/gospacex/otelx/propagation"
	"github.com/gospacex/logx"
	"github.com/gospacex/mqx"
	"github.com/gospacex/otelx/layer"
	"go.opentelemetry.io/otel/attribute"
	otelprop "go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

// 事件状态
//
// ⚠️ 三个状态构成一条**行级认领**链（2026-10-10 加固，见 Runner.claim）：
// pending → claimed → sent / abandon。中间的 claimed 不是「sent 的别名」，
// 它表示「本副本正在发这一行」—— 少了它，同一批行会被多个副本同时捞到并各发
// 一次（markSent 的 status 守卫只能防「重复标记」，防不住「重复投递」）。
const (
	StatusPending = 0
	StatusSent    = 1
	StatusAbandon = 2
	// StatusClaimed 已被某个副本认领（在飞）。它不是终态：发送成功去 sent、
	// 失败回 pending（retry_count+1）、超限去 abandon。
	StatusClaimed = 3
)

// 补发参数（沿用 hubx 既有纪律：3s ticker / 单轮 50 条 / 重试 10 次放弃）
const (
	DefaultPollInterval = 3 * time.Second
	DefaultBatchLimit   = 50
	DefaultRetryCap     = 10
	// DefaultClaimLease 认领租约：claimed 行的 updated_at 超过这个时长仍未被
	// 推进状态，就认为持有它的副本已经死掉，允许别人重新认领。
	//
	// 为什么用租约而不是「认领标记 + 手工清理」：崩溃的进程不会自己回退状态，
	// 没有租约就意味着一行 claimed 会永远卡住（事件静默不再投递）。租约取 60s：
	// 远大于一次 kafka 投递的超时（秒级），因此正常路径下不会被别的副本抢走；
	// 同时远小于「人工发现→介入」的时间尺度，故障后能自愈。
	DefaultClaimLease = time.Minute
	// DefaultSendTimeout 单次投递的 ctx 上限。
	//
	// 为什么必须有：2026-10-10 之前 sendOne 直接吃 FlushOnce 的
	// `context.Background()` —— 没有 deadline，kafka 不可达时 producer.Send
	// 会一直挂在重连上，`FlushOnce` 永不返回，`loop` 也就永远等不到下一个
	// `<-r.stop`；于是 `Stop()` 阻塞在 `<-r.done`，把整个退出链拖死（App.Close
	// 的 stop_timeout 兜不住它，因为超时只是「记一笔警告继续走」，进程退不掉）。
	//
	// 取 5s：明显大于一次正常 kafka 投递（毫秒到百毫秒级），不会误杀正常发送；
	// 相对 3s 的轮询间隔也不会让积压时每轮都被超时截断。
	DefaultSendTimeout = 5 * time.Second
	// DefaultStopGrace Stop() 等待在飞一轮的上限。
	//
	// 取 SendTimeout + 2s：一轮里最坏情况是「拉到一批待发 + 每条都撞上
	// SendTimeout」，但轮内是串行的，所以上界按单条算最坏值再留一点簿记时间。
	// 超时后**放弃等待**而不是继续阻塞：继续等就是把「MQ 挂了」变成「进程退
	// 不掉」，而退出链的设计前提是每一环都有界。
	DefaultStopGrace = DefaultSendTimeout + 2*time.Second
)

// TraceHeaderKey 随事件一起落库的 traceparent 头名（幂等键之外的第二样东西）
const TraceHeaderKey = "traceparent"

// Event 事务发件箱行。各服务的库里都有一张同名表 outbox。
type Event struct {
	ID          uint64    `gorm:"primaryKey;column:id"`
	Topic       string    `gorm:"column:topic"`
	EventType   string    `gorm:"column:event_type"`
	Payload     string    `gorm:"column:payload"`
	Traceparent string    `gorm:"column:traceparent"`
	Status      int       `gorm:"column:status"`
	RetryCount  int       `gorm:"column:retry_count"`
	CreatedAt   time.Time `gorm:"column:created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
}

// TableName 显式表名
func (Event) TableName() string { return "outbox" }

// Config 补发器参数
type Config struct {
	// PollInterval 轮询间隔，<=0 取 DefaultPollInterval
	PollInterval time.Duration
	// BatchLimit 单轮补发上限（防积压时一次性打爆 broker），<=0 取 DefaultBatchLimit
	BatchLimit int
	// RetryCap 重试上限，超过即放弃（status=2，人工对账兜底），<=0 取 DefaultRetryCap
	RetryCap int
	// ClaimLease 认领租约，<=0 取 DefaultClaimLease（见该常量的取舍说明）
	ClaimLease time.Duration
	// SendTimeout 单次投递的 ctx 上限，<=0 取 DefaultSendTimeout。
	// 它同时是退出链是否有界的依据（见 DefaultSendTimeout 的说明）。
	SendTimeout time.Duration
	// StopGrace Stop() 等待在飞一轮的上限，<=0 取 DefaultStopGrace
	StopGrace time.Duration
	// TraceOn 是否产 messaging span（对应配置 trace.on.mq）
	TraceOn bool
}

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.BatchLimit <= 0 {
		c.BatchLimit = DefaultBatchLimit
	}
	if c.RetryCap <= 0 {
		c.RetryCap = DefaultRetryCap
	}
	if c.ClaimLease <= 0 {
		c.ClaimLease = DefaultClaimLease
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = DefaultSendTimeout
	}
	if c.StopGrace <= 0 {
		c.StopGrace = DefaultStopGrace
	}
	return c
}

// Runner 事务发件箱补发器。
//
// producer 为 nil（MQ 未装配或降级）时**不启动补发并打 Error**：这种情况下
// 业务仍在持续写 status=0 的事件，却没有任何投递者，表会无声膨胀到爆 ——
// 静默跳过等于埋雷，必须留可观测痕迹。
type Runner struct {
	db       *gorm.DB
	producer mqx.Producer
	logger   logx.Logger
	cfg      Config

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewRunner 构造补发器（producer 允许 nil，见类型注释）
func NewRunner(db *gorm.DB, producer mqx.Producer, logger logx.Logger, cfg Config) *Runner {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &Runner{db: db, producer: producer, logger: logger, cfg: cfg.withDefaults()}
}

// Start 启动轮询补发（幂等；producer 为 nil 时不启动并打 Error）
func (r *Runner) Start() {
	r.once.Do(func() {
		if r.producer == nil {
			r.logger.Error("outbox: no producer, relay disabled - events will accumulate unflushed")
			return
		}
		r.stop = make(chan struct{})
		r.done = make(chan struct{})
		go r.loop()
	})
}

// Stop 停止补发（等待在飞的一轮，最长 Config.StopGrace；未 Start 时 no-op）
//
// 等待必须**有界**（2026-10-10 修）：此前这里是无条件 `<-r.done`，而 loop 只有在
// 下一轮 select 时才看得到 stop；一旦 FlushOnce 卡住（旧实现里 sendOne 用的是
// 无 deadline 的 `context.Background()`），done 永不关闭，Stop 就永远不返回 ——
// 退出链上多出一个不受 stop_timeout 约束的环节，进程退不掉。
//
// 超时后放弃等待而不是继续阻塞：退出链的设计前提是每一环都有界。残留的
// claimed 行不是数据丢失，租约（ClaimLease）到期后会被别的副本重新认领。
func (r *Runner) Stop() {
	if r.stop == nil {
		return
	}
	select {
	case <-r.done:
		return
	default:
		close(r.stop)
	}
	select {
	case <-r.done:
	case <-time.After(r.cfg.StopGrace):
		r.logger.Warn("outbox: stop grace exceeded, leaving the in-flight flush behind",
			logx.String("grace", r.cfg.StopGrace.String()),
			logx.String("consequence", "claimed rows stay claimed until ClaimLease expires"),
			logx.String("hint", "usually means the broker is unreachable; check mqx producer health"))
	}
}

func (r *Runner) loop() {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			close(r.done)
			return
		case <-ticker.C:
			r.FlushOnce()
		}
	}
}

// FlushOnce 单轮补发（导出供单测直接驱动，避免依赖 ticker 时序）
//
// 一轮的顺序：pending（含租约过期的在飞行）→ 逐行 claim（CAS）→ 投递 →
// markSent / markRetry / abandon。认领必须在**投递之前**，且以 UPDATE 的
// RowsAffected 为准 —— 顺序或判据一改，「多副本不重复投递」就没了。
//
// producer 为 nil 时直接返回并记 Error，不 panic：本方法是导出的，Start 的守卫
// 拦不住「有人直接调它」。
func (r *Runner) FlushOnce() []uint64 {
	if r.producer == nil {
		r.logger.Error("outbox: flush called without a producer, nothing sent")
		return nil
	}
	// 空转轮询用免追踪 ctx：轮询查询没有上游请求，若产 span 会每 3s 另起一根
	// 孤儿单 span trace（db.query），Jaeger 列表被噪声淹没。只有真拉到待发
	// 事件时才恢复可追踪 —— 投递与簿记分别用什么 ctx 见下方 txCtx / bookCtx。
	ctx := orm.WithoutTracing(context.Background())
	pending, err := r.pending(ctx, r.cfg.BatchLimit)
	if err != nil {
		r.logger.ErrorContext(ctx, "outbox: list pending failed", logx.Err(err))
		return nil
	}
	if len(pending) == 0 {
		return nil
	}
	// 投递用可追踪 ctx（txCtx）：sendOne 内有 traceparent 时会 Extract 并把
	// send/kafka.send 挂回原下单链 —— 这是 outbox 与业务 trace 的唯一汇合点。
	// 簿记回写（markSent / markRetry / abandon）用免追踪 ctx（bookCtx）：它们是
	// outbox 表的内部状态迁移，不挂任何父 span，放出来只会各自成为根 —— 发送
	// 成功时是一根孤零零的 db.update，失败重试时更是每轮一根。空转免追踪防的
	// 是「没事件也产 span」，这里防的是「事件发完了，簿记痕迹在 Jaeger 上成了
	// 主角」。
	txCtx := context.Background()
	bookCtx := orm.WithoutTracing(context.Background())
	sent, retried := make([]uint64, 0, len(pending)), make([]uint64, 0)
	var failed []Event
	for i := range pending {
		ev := &pending[i]
		// 行级认领（2026-10-10 加固）：多副本下同一行会被几个 Runner 同时捞到，
		// 只有 CAS 成功的那个才有权投递它。这一步是「不重复投递」的**全部**依据
		// —— 后面的 mark* 守卫只能防「重复标记」，防不住「重复投递」。
		//
		// 认领用免追踪 ctx：它是 outbox 表的内部状态迁移，与 mark* 同性质。
		owned, cerr := r.claim(bookCtx, ev.ID)
		if cerr != nil {
			r.logger.ErrorContext(txCtx, "outbox: claim failed", logx.Int("id", int(ev.ID)), logx.Err(cerr))
			continue
		}
		if !owned {
			// 被别的副本抢走，或这一行已被推进到别的状态：让给它，本轮不发。
			continue
		}
		if ev.Payload == "" {
			// 事件体为空（上游 marshal 失败已记 warn）：直接放弃，不占用重试配额
			r.logger.WarnContext(txCtx, "outbox: empty payload, abandoning", logx.Int("id", int(ev.ID)))
			if err := r.abandon(bookCtx, []uint64{ev.ID}); err != nil {
				r.logger.ErrorContext(txCtx, "outbox: abandon empty-payload event failed", logx.Err(err))
			}
			continue
		}
		if err := r.sendOne(txCtx, ev); err != nil {
			r.logger.WarnContext(txCtx, "outbox: send failed, will retry",
				logx.Int("id", int(ev.ID)), logx.Err(err))
			retried = append(retried, ev.ID)
			failed = append(failed, *ev)
			continue
		}
		sent = append(sent, ev.ID)
	}
	if len(sent) > 0 {
		if err := r.markSent(bookCtx, sent); err != nil {
			r.logger.ErrorContext(txCtx, "outbox: mark sent failed", logx.Err(err))
		}
	}
	if len(retried) == 0 {
		return nil
	}
	// 顺序不能反过来（2026-10-10 修）：先 abandon、再 markRetry。
	//
	// abandon 的守卫是 `status = claimed`，而 markRetry 会把行**退回 pending**
	// —— 先 markRetry 再 abandon，守卫全部落空，「放弃」会静默变成 no-op：
	// 到了重试上限的事件会永远在 pending/claimed 之间循环，永不放弃。
	// 反过来则两条 UPDATE 各自精确命中自己的目标集（已 abandon 的行不满足
	// markRetry 的 claimed 守卫，自然被跳过）。
	abandoned := AbandonCandidates(failed, r.cfg.RetryCap)
	if len(abandoned) > 0 {
		if err := r.abandon(bookCtx, abandoned); err != nil {
			r.logger.ErrorContext(txCtx, "outbox: abandon failed", logx.Err(err))
		} else {
			r.logger.WarnContext(txCtx, "outbox: retry cap reached, abandoned for manual reconcile",
				logx.Int("count", len(abandoned)))
		}
	}
	if err := r.markRetry(bookCtx, retried); err != nil {
		r.logger.ErrorContext(txCtx, "outbox: mark retry failed", logx.Err(err))
		return nil
	}
	return abandoned
}

// AbandonCandidates 从**本轮发送失败**的事件里挑出重试已达上限的那些。
//
// 入参刻意只接收失败集合。若遍历整个 pending，「重试 9 次、第 10 次刚好成功」
// 的事件会因 9+1>=cap 被一起塞进放弃名单。下层 mark*/abandon 都带
// `status = pending` 守卫，所以这个误判不会真的把行改成放弃态，但它会：
//  1. 打出与事实不符的 "abandoned count=N" 告警，运维按数对账会对不上；
//  2. 每轮多一次覆盖全表的 UPDATE；
//  3. 一旦守卫放宽，立刻从噪声升级为数据损坏。
//
// 独立纯函数是为了让这条语义能被单测直接钉住。
func AbandonCandidates(failed []Event, cap int) []uint64 {
	out := make([]uint64, 0, len(failed))
	for i := range failed {
		if failed[i].RetryCount+1 >= cap {
			out = append(out, failed[i].ID)
		}
	}
	return out
}

// sendOne 单条投递。续链分两段职责，别混在一起：
//
//  1. **还原上下文**（还原成本极低，且与开关无关）—— 只要落库时留了 traceparent
//     就 Extract。Extract 只往 ctx 里放一个 remote span context，不产生 span。
//  2. **开 outbox.send span** —— 这才是埋点，受 TraceOn 管；关掉时链路照样由第
//     1 步接上，只是少一层 hop。
//
// header 不再手写：producer.Send 内部会按 ctx 自动 Inject（mqx.InjectTrace），
// 所以这里只要保证 ctx 站对位置即可。轮询循环的 ctx 本身是干净的 root，若不做
// 第 1 步，这条链就断在 broker 这一跳。
func (r *Runner) sendOne(ctx context.Context, ev *Event) error {
	key := fmt.Sprintf("%d", ev.ID)
	// Topic 刻意不写进消息体：producer 由 xmiddleware.mqx 构造时已绑定目标
	// topic，kafka-go 会拒绝 writer 与 message 同时带 topic（实测报
	// "Topic must not be specified for both Writer and Message"）。
	msg := &mqx.Message{
		Key:     key,
		Value:   []byte(ev.Payload),
		Headers: map[string]string{},
	}
	if ev.Traceparent != "" {
		ctx = Extract(ctx, otelprop.MapCarrier{TraceHeaderKey: ev.Traceparent})
	}
	if r.cfg.TraceOn && ev.Traceparent != "" {
		var span trace.Span
		ctx, span = layer.Start(ctx, "messaging", "outbox", "send")
		defer span.End()
		span.SetAttributes(attribute.String("messaging.topic", ev.Topic))
	}
	// 投递必须有 deadline（2026-10-10 修）。
	//
	// 用 WithTimeout **派生**而不是另起 context.Background()：ctx 上带着上一步
	// Extract 出来的父 span 上下文，重起一个会直接把这条事件从原下单 trace 上
	// 摘掉 —— 那正是 outbox 存在的意义（补发也要挂回原链）。派生保留 values 与
	// span，只额外加一个 deadline。
	//
	// 超时后 producer.Send 返回 ctx 错误，走 FlushOnce 的 retry 分支
	// （retry_count+1，超 RetryCap 转 abandon）；这一轮也能正常结束，
	// Stop() 不会再被拖住。
	sendCtx, cancel := context.WithTimeout(ctx, r.cfg.SendTimeout)
	defer cancel()
	return r.producer.Send(sendCtx, msg)
}

// claim 把一行推进为「本副本在飞」。返回 false 表示这一行不归本轮投递。
//
// 判据是**单条条件 UPDATE 的 RowsAffected**（CAS），不是先查后改：
// 「先 SELECT 看 status 再 UPDATE」存在 check-then-act 窗口 —— 两个副本都能读到
// status=0，然后各发一次；把这个窗口关掉正是本函数存在的唯一理由。
//
// 可认领的两种行：待发（pending），以及租约已过期（claimed 但 updated_at 太老）
// —— 后者是给「持有它的副本崩了」留的自愈路径，没有它，一次崩溃会让那行永久卡住。
func (r *Runner) claim(ctx context.Context, id uint64) (bool, error) {
	deadline := time.Now().Add(-r.cfg.ClaimLease)
	res := r.db.WithContext(ctx).Model(&Event{}).
		Where("id = ? AND (status = ? OR (status = ? AND updated_at < ?))",
			id, StatusPending, StatusClaimed, deadline).
		Update("status", StatusClaimed)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, nil
}

// pending 取**可认领**的事件：待发（status=0）或认领租约已过期者。
//
// 租约比较用的是 Go 侧算出的时间戳（deadline），不是 SQL 的 NOW()：后者在各
// 驱动/方言里写法不一（MySQL 的 NOW() - INTERVAL、sqlite 的 datetime()），
// 而租约是个粗粒度阈值（分钟级），几毫秒的应用/数据库时钟偏差不影响判断。
func (r *Runner) pending(ctx context.Context, limit int) ([]Event, error) {
	deadline := time.Now().Add(-r.cfg.ClaimLease)
	var rows []Event
	err := r.db.WithContext(ctx).
		Where("status = ? OR (status = ? AND updated_at < ?)",
			StatusPending, StatusClaimed, deadline).
		Order("id ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// markSent 标记已发（幂等：仅**本轮认领到的**行可迁移到 sent）
func (r *Runner) markSent(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&Event{}).
		Where("id IN ? AND status = ?", ids, StatusClaimed).
		Update("status", StatusSent).Error
}

// markRetry 补发失败：retry_count+1，并把行**退回 pending**。
//
// 退回而不是留在 claimed 是刻意的：留在 claimed 只能等租约过期才被重新捞到，
// 每次失败都会白白多等一个租约（默认 60s）；退回 pending 则下轮（3s 后）即可重试，
// 保持与加固前一致的失败重试节奏。
func (r *Runner) markRetry(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&Event{}).
		Where("id IN ? AND status = ?", ids, StatusClaimed).
		Updates(map[string]any{
			"status":      StatusPending,
			"retry_count": gorm.Expr("retry_count + 1"),
		}).Error
}

// abandon 重试超限：放弃（status=2，人工对账兜底）
func (r *Runner) abandon(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&Event{}).
		Where("id IN ? AND status = ?", ids, StatusClaimed).
		Update("status", StatusAbandon).Error
}

// Extract 从入向 carrier 提取上游 span context，返回携带该 context 的 ctx。
//
// 库侧入口（re-export 自 otelx/propagation，语义完全一致）：调用方写
// outbox 行时要用它把当前 traceparent 落库，补发器再把它接回原链。直接引
// otelx/propagation 也可以 —— 此入口只是少引一个包的便捷形式，勿在此加行为。
func Extract(ctx context.Context, carrier otelprop.TextMapCarrier) context.Context {
	return otelxprop.Extract(ctx, carrier)
}
