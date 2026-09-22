# retry3 现网契约与 admission 修复兼容合并

2026-09-22 泽鱼明确确认：保留现网新面板、参数、路由，再合入已验修复并验收。真实调用上限1000 Kiro credits，当前0消耗。本文覆盖旧spec里与最新现网冲突的默认值；不修改账号/RPM实值/计价/inject/sub2api。

## 基线与边界

- 已验修复：5349799（740单测、31隔离镜像测试），最新文档13c5cf6。
- 最新生产：retry3，image6ab96576…，binary10df987d…；源码c7fa179+新增面板/ConcurrencyGate/所有fable路由/三次尝试。09-22 08:01Z再次只读确认源码diff1a181541…未变。
- 主会话已原样导入live UI、Admin、concurrency设置类型、gate、main、模型映射；在handlers的动态模型列表追加两条fable5别名。**这是未完成接线的种子，不声称当前可编译或可上线。**
- 保留User UI/Admin现有缓存计价功能。不得覆盖已验typed错误、原子RPM、原始bound/model终态、OAuth准备任务、LeasedResponse和搜索取消修复。

## 必须保留的新现网契约

1. GET/PUT `/api/admin/config/concurrency` 与设置面板热改、落盘保持，字段名/默认值/sanitize行为来自live ConcurrencySettings。
2. 单号容量默认5，0表示不限单号；没有隐藏的20上限。全局完整流上限仍由`maxConcurrentRequests`控制（默认50）。
3. 账号指数退避、suspended等待、全局429冷却5/15/30–60秒及120秒空闲复位保持；晚到成功不能清除较新的429代次。
4. 所有实际发送总计最多3次；无有效Retry-After的`INSUFFICIENT_MODEL_CAPACITY`最多2次，同号同端点，不增加账号轮转/故障计数、不封端点桶。Gate的账号/全局退避仍更新，与live一致。
5. 全部fable别名映射到claude-opus-5；动态模型列表/Admin别名与费率来源保持。
6. 实际挂载配置RPM20与cache-split0.1768不改；代码只改准入与传播语义。

## 容量只有一个权威

- ConcurrencyGate是**唯一**账号活跃流计数与动态容量来源；删除provider的账号Semaphore/map/per_credential_limit，不能叠加两个上限。
- 在LeasedResponse内持有全局OwnedSemaphorePermit和Gate InFlightGuard，直到body EOF/Err/Drop/text/bytes完成。保留现有 `(LeasedResponse,id)` 返回类型，不能退回live裸Response加散落guard方案。
- 取得许可必须非阻塞组合，没有await夹在两种许可之间；失败立即归还另一种。RPM在完整请求准备及许可之后、真正send之前原子预留；失败释放所有活跃槽。
- Gate提供非阻塞准入结果（GlobalCooldown(until)/AccountBackoff(until)/Full），以及可重新检查的状态变化通知。通知必须先注册/enable再检查，避免归还/热改瞬间丢唤醒。Drop、设置变化、退避变化要唤醒对应扫描者。
- 满/退避账号先尝试其他合格账号；只是临时阻塞，若等待期间恢复，应可重新扫描，不应永久hard_avoid或惩罚健康轮转偏置。bound/model资格和空集合不扩权仍是硬约束。
- 全局满/全池暂时满时可在同一准入deadline内等变化；不持任一活跃许可等待。不要在仅账号阻塞时等待已经有空位的global semaphore，避免自激忙循环。
- 单号热改5→0立即允许超过20（只受全局限额）；调低不会撤销已发流，只禁止新增，不重置在途计数。

## 配置兼容

嵌套 `concurrency` 是新现网权威。允许保留未发布候选的顶层 `maxConcurrentPerCredential` 作为仅启动时的兼容alias：

- 嵌套对象存在（包括 `{}`）→使用它；否则显式顶层alias→作为初始单号cap；都未配置→live默认5。
- alias的0也可表达不限单号；全局maxConcurrentRequests和准入票仍必须非零。
- 可用 `Option<ConcurrencySettings>` 和 `Option<usize>` + `effective_concurrency_settings()` 实现，避免重复整个Config结构做手工反序列化。main统一用effective设置创建共享Gate；Admin和provider指向**同一Arc**。
- Admin保存写入嵌套对象，后续重启嵌套优先，不被旧alias覆盖。不增加无关配置项/新crate。

## 冷却、截止时间与错误

- 仍是一张入口准入票、同一个deadline，换号、端点、等待不重置。Token准备使用已验独立有界任务，客户超时不丢OAuth轮换结果；不得回退直接timeout/drop刷新。
- 冷却/容量等待不占活跃槽；超出deadline时返回429及**当时仍需等待的时长**。同一候选的串联限制取较晚截止，可替代候选取最早可恢复时间；全局冷却约束所有候选。
- 任何真实上游429恰好一次更新Gate，包括有有效Retry-After而不读取body的早返回；本地RPM/queue拒绝不是上游429，不触发全局阶梯惩罚。
- 有有效Retry-After的模型/MCP429仍立刻登记对应桶并返回，不等错误正文、不为凑3次继续发包。Gate全局策略也必须保留；合法上游明确等待不能被更短本地值覆盖。
- 容量429无头专门处理；其他429保留既有端点避让/soft fallback。默认首个全局5秒与准入5秒相同，因此容量第二枪可能来不及发：这是“最多2”，不得延长deadline保证补枪。
- 同一请求实际send预算≤3，扫描候选不扣预算；历史子集quota标记不能证明原始绑定组全部耗尽。原始范围真实402 > 真实限流 > 历史上游错误 > 新busy。

## 合并验收

C01 配置缺省5、嵌套优先、显式alias、嵌套/alias零、热改保存后重启取新值。
C02 GET/PUT接口及新面板资源保留；cache-split/账号/其他配置不被覆盖。
C03 全局与Gate计数覆盖完整流；EOF/Err/cancel释放；0时同号>20，global仍有效；热改增减容量不丢计数。
C04 满A但B可用；全满后释放/热改唤醒；同一个deadline，通知竞态无死锁/忙循环；原scope永不因空集变成全局。
C05 冷却时无活跃槽；全局暂停影响其他账号；热关解除全局窗但不误清账号退避；超时返回实际剩余而非1秒。
C06 有Retry-After的模型/MCP早返回仍触发Gate且不等慢body；无头容量同号同端点≤2，所有错误混合实际send≤3。
C07 旧成功不能清新429；RPM失败尝试计数、OAuth轮换保存与有界准备、四入口/搜索取消不回归。
C08 全fable5/5.1/thinking映射opus5；计价相关源码及前端资源保留。

原31镜像核心用例可显式关闭新增可选全局/账号退避，以继续隔离测试原子RPM、端点fallback等机制；必须另加默认/开启策略及管理热改用例，不能仅关策略刷绿。单测旧断言只允许因本规范明确的默认5/最多3/新Guard类型而做等价适配；不能删掉失败路径或ignore。

## 发布门

合并完成后独立review、真实测试和原生amd64镜像套件；新包冻结后再持服务器锁/CAS/备份，做≤1000credits真验。不上旧534包，不共用可刷新生产凭据/可写卷，不部署未测试镜像。源代码及配置现场若再变，先评估差异，不覆盖。
