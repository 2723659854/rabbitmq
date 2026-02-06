<?php

namespace Xiaosongshu\Rabbitmq;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * @purpose rabbitmq客户端
 * @author yanglong
 * @time 2025年9月29日15:25:47
 * @note 客户端必须实现handle,dlxHandle,error方法
 */
abstract class Client implements RabbiMQInterface
{
    /** @var string 服务器地址 */
    public static $host = "";
    /** @var int 服务器端口 */
    public static $port = 5672;
    /** @var string 服务器登陆用户 */
    public static $user = "guest";
    /** @var string 服务器登陆密码 */
    public static $pass = "guest";

    /**
     * @var array 存储每个队列的独立信道（key：connectKey，value：AMQPChannel）
     * @note 新增：与connections一一对应，避免信道共用导致数据混乱
     */
    protected static $channels = [];
    /**
     * @var array 存储每个队列的独立连接（key：connectKey，value：AMQPStreamConnection）
     * @note 原有：已实现按key区分连接
     */
    protected static $connections = [];

    /** @var int 过期时间 */
    public static $timeOut = 0;
    /** @var string 交换机名称（子类可自定义，未定义则用类名） */
    public static $exchangeName = "";
    /** @var string 队列名称（子类可自定义，未定义则用类名） */
    public static $queueName = "";
    /** @var string $type 分发方式 */
    public static $type = "";
    /** @var string 交换机类型 转发给所有绑定到本交换机的通道，不匹配路由 */
    const EXCHANGETYPE_FANOUT = "fanout";
    /** @var string 交换机类型 只转发给绑定到本交换机，并且路由完全匹配的通道 */
    const EXCHANGETYPE_DIRECT = "direct";
    /** @var string 交换机类型  延迟信息 */
    const EXCHANGETYPE_DELAYED = "x-delayed-message";
    /** 消费成功 */
    const ACK = 1;

    /** 消费失败，重复投递 */
    const NACK = 2;
    /** 消费失败，支持重复投递一次 */
    const REJECT = 3;

    /** 重新连接最大尝试次数，可能rabbitmq服务网络跳动等异常情况，所以设置大一点 */
    protected static $maxRetryConnect = 5;

    /** 已经尝试重连次数 */
    protected static $hasRetryConnect = 0;

    /** 心跳间隔 */
    public static $heartbeat = 60;

    /** 设置本次消费个数 */
    protected static $total = 0;

    /** 当前队列剩余消费个数 */
    protected static $remain = 0;

    /** 是否开启死信队列 */
    public static $enableDlx = false;
    /** 死信队列交换机（未定义则用「业务交换机+_dlx」） */
    public static $dlxExchangeName = "";
    /** 死信队列名称（未定义则用「业务队列+_dlx」） */
    public static $dlxQueueName = "";
    /** 死信队列路由（未定义则用「业务队列+_dlx」） */
    public static $dlxRoutingKey = "";
    /** 死信消息生命周期 */
    public static $dlxMessageTtl = 0;
    /** 死信队列进程id */
    protected static $dlxPid = 0;
    /** 操作系统不是windows */
    protected static $IS_NOT_WINDOWS = false;

    /**
     * @var array 存储每个队列的连接状态（key：connectKey，value：bool）
     * @note 原有：已实现按key区分连接中状态
     */
    protected static $connecting = [];

    /**
     * 判断是否是windows系统
     * @return bool
     */
    protected static function isNotWindows()
    {
        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            static::$IS_NOT_WINDOWS = false;
        } else {
            static::$IS_NOT_WINDOWS = true;
        }
        return static::$IS_NOT_WINDOWS;
    }

    /**
     * 获取队列唯一标识（交换机+队列名）
     * @return array [交换机名, 队列名, 连接key]
     * @note 原有：逻辑保留，确保每个队列的key唯一
     */
    protected static function getQueueName()
    {
        // 动态生成交换机名：子类未定义则用类名
        $exchangeName = static::$exchangeName ?: static::class;
        // 动态生成队列名：子类未定义则用类名
        $queueName = static::$queueName ?: static::class;
        // 连接key：确保唯一（交换机名@队列名）
        $connectKey = $exchangeName . '@' . $queueName;

        return [$exchangeName, $queueName, $connectKey];
    }

    /**
     * 连接服务器并创建队列（每个队列独立连接/信道）
     * @return bool 连接+绑定是否成功
     * @note 核心完善：按connectKey管理连接/信道，确保每个队列独立绑定
     */
    private static function make()
    {
        // 1. 获取当前队列的唯一标识
        [$exchangeName, $queueName, $connectKey] = static::getQueueName();
        /** 检测操作系统 */
        static::isNotWindows();

        // 2. 检查是否已连接：若连接有效且信道打开，直接返回
        if (static::isConnected($connectKey)) {
            return true;
        }

        // 3. 处理并发连接：避免同一队列重复创建连接
        // 初始化连接状态（未定义则设为false）
        if (!isset(static::$connecting[$connectKey])) {
            static::$connecting[$connectKey] = false;
        }
        // 若正在连接中，等待直至连接完成（最多等待5秒，避免死等）
        $waitCount = 0;
        while (static::$connecting[$connectKey] && $waitCount < 50) {
            \usleep(100000); // 每次等待100ms
            $waitCount++;
        }
        // 等待后仍在连接中，视为失败
        if (static::$connecting[$connectKey]) {
            static::error(new \RuntimeException("队列[{$queueName}]连接中，请勿重复请求"));
            return false;
        }

        // 4. 标记当前队列开始连接
        static::$connecting[$connectKey] = true;
        // 先清理当前队列的旧资源（避免残留连接/信道）
        static::close($connectKey);
        // 初始化分发方式（未定义则用direct）
        $exchangeType = static::$type ?: static::EXCHANGETYPE_DIRECT;

        // 5. 处理死信队列的动态标识（未定义则自动生成）
        $dlxExchangeName = static::$dlxExchangeName ?: $exchangeName . "_dlx";
        $dlxQueueName = static::$dlxQueueName ?: $queueName . "_dlx";
        $dlxRoutingKey = static::$dlxRoutingKey ?: $queueName . "_dlx";

        try {
            // 6. 创建当前队列的独立连接
            static::$connections[$connectKey] = new AMQPStreamConnection(
                static::$host, static::$port, static::$user, static::$pass,
                '/', false, 'AMQPLAIN', null, 'en_US', 1.0,
                3.0, null, true, static::$heartbeat, 0.0, null, null
            );

            // 7. 创建当前队列的独立信道（每个连接对应一个信道）
            $channel = static::$connections[$connectKey]->channel();
            static::$channels[$connectKey] = $channel; // 存入信道数组

            // 8. 声明当前队列的业务交换机（幂等操作：已存在则忽略）
            $exchangeArguments = new AMQPTable();
            // 若为延迟交换机，补充 x-delayed-type 参数
            if ($exchangeType === static::EXCHANGETYPE_DELAYED) {
                $exchangeArguments->set('x-delayed-type', static::EXCHANGETYPE_DIRECT); // 底层转发类型
            }

            $channel->exchange_declare(
                $exchangeName,  // 交换机名称
                $exchangeType,  // 交换机类型
                false,          // passive
                true,           // durable
                false,          // auto_delete
                false,          // internal
                false,          // nowait
                $exchangeArguments
            );

            // 9. 准备业务队列参数（含死信配置）
            $queueArguments = new AMQPTable();
            if (static::$enableDlx) {
                $queueArguments->set('x-dead-letter-exchange', $dlxExchangeName);
                $queueArguments->set('x-dead-letter-routing-key', $dlxRoutingKey);
                $queueArguments->set('x-message-ttl', 30000); // 业务消息30秒过期
            }

            // 10. 声明并绑定业务队列（幂等操作）
            try {
                // 声明业务队列
                $channel->queue_declare(
                    $queueName,     // 用动态生成的队列名（非static::$queueName）
                    false,          // passive：不检查是否存在
                    true,           // durable：持久化
                    false,          // exclusive：不排他（多消费者可访问）
                    false,          // auto_delete：不自动删除
                    false,
                    $queueArguments
                );
                // 绑定队列到交换机（路由键用队列名，确保精准匹配）
                $channel->queue_bind(
                    $queueName,
                    $exchangeName,
                    $queueName
                );
            } catch (\Exception|\Throwable $exception) {
                static::error(new \RuntimeException("队列[{$queueName}]绑定失败：" . $exception->getMessage()));
                throw $exception; // 重新抛出，触发重连逻辑
            }

            // 11. 处理死信队列（仅在启用时，使用当前队列的连接创建死信信道）
            if (static::$enableDlx) {
                // 创建死信专用信道（避免与业务信道混用）
                $dlxChannel = static::$connections[$connectKey]->channel();
                try {
                    // 声明死信交换机
                    $dlxChannel->exchange_declare(
                        $dlxExchangeName,
                        static::EXCHANGETYPE_DIRECT,
                        false,
                        true,
                        false
                    );

                    // 准备死信队列参数
                    $dlxQueueArguments = new AMQPTable();
                    if (static::$dlxMessageTtl > 0) {
                        $dlxQueueArguments->set('x-message-ttl', static::$dlxMessageTtl);
                    }

                    // 声明死信队列
                    $dlxChannel->queue_declare(
                        $dlxQueueName,
                        false,
                        true,
                        false,
                        false,
                        false,
                        $dlxQueueArguments
                    );

                    // 绑定死信队列
                    $dlxChannel->queue_bind(
                        $dlxQueueName,
                        $dlxExchangeName,
                        $dlxRoutingKey
                    );
                } catch (\Exception|\Throwable $exception) {
                    static::error(new \RuntimeException("死信队列[{$dlxQueueName}]绑定失败：" . $exception->getMessage()));
                    throw $exception;
                } finally {
                    // 死信队列声明后关闭信道（消费时会重新创建专属信道）
                    if ($dlxChannel->is_open()) {
                        $dlxChannel->close();
                    }
                }
            }

            // 12. 连接成功：重置重连次数，标记连接完成
            static::$hasRetryConnect = 0;
            static::$connecting[$connectKey] = false;
            return true;

        } catch (\Exception $exception) {
            // 13. 连接/绑定失败：清理资源，触发重连
            static::$connecting[$connectKey] = false;
            static::close($connectKey); // 清理当前队列的旧资源

            // 重试逻辑：消费者必须一直尝试连接，确保业务数据被处理，除非管理员手动关闭队列进程
            if (static::$maxRetryConnect) {
                static::$hasRetryConnect++;
                $sleepTime = static::getSleepTime();
                static::error(new \RuntimeException("队列[{$queueName}]连接失败（第" . static::$hasRetryConnect . "次重试）：" . $exception->getMessage() . "，将等待{$sleepTime}秒后重试"));
                \sleep($sleepTime);
                return static::make(); // 递归重连
            } else {
                // 投递消息不可重试，连接失败不处理，防止阻塞其他业务，异常情况使用error通知管理员，由人工干预
                static::error(new \RuntimeException("队列[{$queueName}]重连达最大次数（" . static::$maxRetryConnect . "次），已停止重试"));
                return false;
            }
        }
    }

    /**
     * 开启死信队列消费
     * @return void
     * @note 主要适用于于windows，简化的消费者方法名称，方便调用
     */
    public static function consumeD()
    {
        static::startDlxConsumer();
    }

    /**
     * 开启死信队列消费
     * @return void
     * @note 完善：使用当前队列的connectKey，确保死信消费与业务队列对应
     */
    private static function startDlxConsumer()
    {
        // 1. 未启用死信队列则退出
        if (!static::$enableDlx) {
            return;
        }
        // 2. 获取当前队列的唯一标识
        [$exchangeName, $queueName, $connectKey] = static::getQueueName();
        $dlxQueueName = static::$dlxQueueName ?: $queueName . "_dlx";

        // 3. Windows系统：直接启动消费（无进程管理）
        if (!static::$IS_NOT_WINDOWS) {
            static::runDlxConsumer($connectKey, $dlxQueueName);
            return;
        }

        // 4. Linux系统：通过子进程管理死信消费（避免阻塞业务进程）
        // 检查子进程是否存活（已存活则不重复创建）
        if (static::$dlxPid > 0 && \posix_kill(static::$dlxPid, 0)) {
            static::error(new \RuntimeException("死信队列[{$dlxQueueName}]子进程已存在（PID：" . static::$dlxPid . "），无需重复创建"));
            return;
        }

        // 创建子进程
        $pid = \pcntl_fork();
        if ($pid == -1) {
            static::error(new \RuntimeException("死信队列[{$dlxQueueName}]创建子进程失败"));
            return;
        } elseif ($pid == 0) {
            // 子进程：设置进程标题，启动死信消费
            \cli_set_process_title("dlx_consumer_" . $dlxQueueName);
            static::runDlxConsumer($connectKey, $dlxQueueName);
            exit(0); // 消费结束后退出子进程
        } else {
            // 父进程：记录子进程ID
            static::$dlxPid = $pid;
            static::error(new \RuntimeException("死信队列[{$dlxQueueName}]子进程创建成功（PID：" . $pid . "）"));
        }
    }

    /**
     * 死信队列消费者逻辑
     * @param string $connectKey 当前队列的连接key
     * @param string $dlxQueueName 死信队列名
     * @return void
     * @note 完善：绑定当前队列的连接/信道，避免消费其他队列的死信
     */
    private static function runDlxConsumer(string $connectKey, string $dlxQueueName)
    {
        $retryCount = 0;
        $maxRetry = 5; // 最大连续重试次数
        $backoffTime = 3; // 初始退避时间（秒）
        while (true) {
            try {
                // 1. 确保当前队列的连接有效（无效则重建）
                if (!static::isConnected($connectKey)) {
                    static::close($connectKey); // 清理旧资源
                    if (!static::make()) { // 重建连接+绑定
                        throw new \RuntimeException("死信队列[{$dlxQueueName}]连接重建失败");
                    }
                    // 连接成功：重置重试计数和退避时间
                    $retryCount = 0;
                    $backoffTime = 3;
                }

                // 2. 获取当前队列的死信消费信道（从connections数组获取）
                $channel = static::$channels[$connectKey];
                if (!$channel || !$channel->is_open()) {
                    throw new \RuntimeException("死信队列[{$dlxQueueName}]信道已关闭");
                }

                // 3. 取消当前队列的旧死信消费者（避免重复消费）
                $dlxConsumerPrefix = $dlxQueueName . "_consumer_";
                foreach ($channel->callbacks as $consumerTag => $_) {
                    if (strpos($consumerTag, $dlxConsumerPrefix) === 0) {
                        $channel->basic_cancel($consumerTag, true);
                        unset($channel->callbacks[$consumerTag]);
                    }
                }

                // 4. 死信消费回调逻辑（仅处理当前队列的死信）
                $dlxCallback = function ($msg) use ($dlxQueueName) {
                    $dlxMsg = \json_decode($msg->body, true);
                    try {
                        // 检查是否实现死信处理方法
                        if (!method_exists(static::class, 'dlxHandle')) {
                            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag'], false);
                            static::error(new \RuntimeException("队列[{$dlxQueueName}]未实现dlxHandle方法，自动确认死信：" . $msg->body));
                            return;
                        }

                        // 执行死信处理逻辑
                        $result = static::dlxHandle($dlxMsg);
                        if ($result === static::ACK) {
                            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag'], false);
                        } else {
                            // 消费失败：重新入队（交给其他死信消费者）
                            $msg->delivery_info['channel']->basic_nack($msg->delivery_info['delivery_tag'], false, true);
                            static::error(new \RuntimeException("队列[{$dlxQueueName}]死信处理失败：" . $msg->body));
                        }
                    } catch (\Throwable $e) {
                        // 致命异常（如代码错误）：拒绝重新入队（避免无限循环）
                        $msg->delivery_info['channel']->basic_nack($msg->delivery_info['delivery_tag'], false, false);
                        static::error(new \RuntimeException("队列[{$dlxQueueName}]死信处理异常：" . $e->getMessage() . "，消息：" . $msg->body));
                    }
                };

                // 5. 配置消费参数（一次处理1条，避免消息堆积）
                $channel->basic_qos(0, 1, false);
                // 绑定死信队列到当前信道
                $channel->basic_consume(
                    $dlxQueueName,
                    $dlxConsumerPrefix . uniqid(), // 唯一消费者标签（避免冲突）
                    false,  // no_local：接收本消费者发送的消息
                    false,  // no_ack：手动确认（避免消息丢失）
                    false,  // exclusive：不排他
                    false,
                    $dlxCallback
                );

                // 6. 持续监听死信消息
                //static::error(new \RuntimeException("死信队列[{$dlxQueueName}]开始监听（PID：" . getmypid() . "）"));
                while (\count($channel->callbacks)) {
                    $channel->wait(null, true, 10);
                    \usleep(1); // 切换CPU，避免占用过高
                }

            } catch (\Exception $e) {
                // 7. 消费异常：重试退避（失败次数越多，等待时间越长）
                $retryCount++;
                $errorMsg = "死信队列[{$dlxQueueName}]消费异常（第{$retryCount}次重试）：" . $e->getMessage();
                static::error(new \RuntimeException($errorMsg));

                // 关闭当前连接（下次重试重建）
                static::close($connectKey);
                // 退避时间：最大30秒
                if ($retryCount >= $maxRetry) {
                    $backoffTime = \min($backoffTime * 2, 30);
                }
                \sleep($backoffTime);
            }
        }
    }

    /**
     * 监控子进程死信队列，但是只能用于linux，不可用于windows
     * @return void
     */
    private static function monitorDlxProcess()
    {
        if (!static::$enableDlx || static::$dlxPid <= 0) {
            return;
        }

        $status = 0;
        $result = \pcntl_waitpid(static::$dlxPid, $status, \WNOHANG);
        if ($result == static::$dlxPid) {
            static::error(new \RuntimeException("死信队列子进程（" . static::$dlxPid . "）已退出，准备重启"));
            static::$dlxPid = 0;
            static::startDlxConsumer(); // 重启死信消费
        }
    }

    /**
     * 开启消费
     * @param int $count 指定本次消费数量
     * @return void
     * @note 完善：使用当前队列的connectKey和信道，避免消费其他队列消息
     */
    public static function consume(int $count = 0)
    {
        // 1. 初始化消费计数
        static::$total = static::$remain = $count;
        static::isNotWindows();
        // 2. 获取当前队列的唯一标识
        [$exchangeName, $queueName, $connectKey] = static::getQueueName();

        // 3. 启动死信消费（仅Linux子进程模式）
        if (static::$enableDlx && static::$IS_NOT_WINDOWS) {
            static::startDlxConsumer();
        }

        // 4. 设置主进程标题（Linux）
        if (empty(static::$queueName)) {
            static::$queueName = $queueName; // 同步队列名（用于进程标题）
        }
        if (static::$IS_NOT_WINDOWS) {
            @\cli_set_process_title("consumer_" . static::$queueName);
        }

        while (true) {
            // 5. 监控死信子进程（Linux）
            if (static::$enableDlx && static::$IS_NOT_WINDOWS) {
                static::monitorDlxProcess();
            }

            // 6. 确保当前队列的连接有效
            if (!static::isConnected($connectKey)) {
                if (!static::make()) {
                    static::error(new \RuntimeException("队列[{$queueName}]连接无效，3秒后重试"));
                    \sleep(3);
                    continue;
                }
            }

            try {
                // 7. 获取当前队列的业务信道
                $channel = static::$channels[$connectKey];
                if (!$channel || !$channel->is_open()) {
                    throw new \RuntimeException("队列[{$queueName}]信道已关闭");
                }

                // 8. 业务消费回调逻辑
                $businessCallback = function ($msg) use ($queueName) {
                    $params = \json_decode($msg->body, true);
                    try {
                        // 检查是否实现业务处理方法
                        if (!\method_exists(static::class, 'handle')) {
                            $msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], false);
                            static::error(new \RuntimeException("队列[{$queueName}]未实现handle方法，拒绝消息：" . $msg->body));
                            return;
                        }

                        // 执行业务逻辑
                        $ack = static::handle($params);
                        if ($ack == static::ACK) {
                            // 消费成功：确认消息
                            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag'], false);
                        } elseif ($ack == static::NACK) {
                            // 消费失败：重新入队
                            $msg->delivery_info['channel']->basic_nack($msg->delivery_info['delivery_tag'], false, true);
                            static::error(new \RuntimeException("队列[{$queueName}]业务处理失败（重新入队）：" . $msg->body));
                        } elseif ($ack == static::REJECT) {
                            // 拒绝消息：重新入队（分配给其他消费者）
                            $msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], true);
                            static::error(new \RuntimeException("队列[{$queueName}]业务处理拒绝（重新入队）：" . $msg->body));
                        }
                    } catch (\Throwable $exception) {
                        // 致命异常：拒绝重新入队（避免无限循环）
                        $msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], false);
                        static::error(new \RuntimeException("队列[{$queueName}]业务处理异常：" . $exception->getMessage() . "，消息：" . $msg->body));
                    }

                    // 9. 递减消费计数（指定消费数量时）
                    if (static::$total > 0) {
                        static::$remain--;
                    }
                };

                // 10. 配置消费参数（一次1条，手动确认）
                $channel->basic_qos(0, 1, false);
                // 绑定业务队列到当前信道
                $channel->basic_consume(
                    $queueName,
                    $queueName . "_consumer_" . uniqid(), // 唯一消费者标签
                    false,
                    false,
                    false,
                    false,
                    $businessCallback
                );

                // 11. 开始消费（按指定数量或持续消费）
                //static::error(new \RuntimeException("队列[{$queueName}]开始消费（PID：" . getmypid() . "），本次消费" . ($count > 0 ? $count . "条" : "持续消费")));
                if (static::$total > 0) {
                    // 指定消费数量：消费完指定条数后退出
                    while (static::$remain > 0 && \count($channel->callbacks)) {
                        if (static::$IS_NOT_WINDOWS) {
                            static::monitorDlxProcess();
                        }
                        $channel->wait(null, true, 10);
                        \usleep(1);
                    }
                    static::error(new \RuntimeException("队列[{$queueName}]指定消费数量已完成（共" . static::$total . "条）"));
                } else {
                    // 持续消费：直到信道关闭或手动停止
                    while (\count($channel->callbacks)) {
                        if (static::$IS_NOT_WINDOWS) {
                            static::monitorDlxProcess();
                        }
                        $channel->wait(null, true, 10);
                        \usleep(1);
                    }
                }

                // 12. 消费完成：退出循环
                break;

            } catch (\PhpAmqpLib\Exception\AMQPConnectionClosedException $exception) {
                // 13. 连接断开：清理后重试
                static::error(new \RuntimeException("队列[{$queueName}]连接已断开：" . $exception->getMessage()));
                static::close($connectKey);
                \sleep(static::getSleepTime());
            } catch (\Throwable $exception) {
                // 14. 其他异常：判断是否可恢复
                static::error(new \RuntimeException("队列[{$queueName}]消费异常：" . $exception->getMessage()));
                $isPossiblyRecoverable = $exception instanceof \PhpAmqpLib\Exception\AMQPRuntimeException;

                if (!$isPossiblyRecoverable) {
                    // 致命异常（如代码错误）：退出进程
                    static::error(new \RuntimeException("队列[{$queueName}]检测到致命错误，进程退出"));
                    break;
                }

                // 可恢复异常：清理后重试
                static::close($connectKey);
                \sleep(static::getSleepTime());
            }
        }

        // 15. 消费结束：清理资源
        // 关闭死信子进程（Linux）
        if (static::$enableDlx && static::$dlxPid > 0 && static::isNotWindows()) {
            \posix_kill(static::$dlxPid, \SIGTERM);
            static::error(new \RuntimeException("队列[{$queueName}]死信子进程（PID：" . static::$dlxPid . "）已关闭"));
        }
        // 关闭当前队列的连接/信道
        static::close($connectKey);
        static::error(new \RuntimeException("队列[{$queueName}]消费进程已退出（PID：" . getmypid() . "）"));
    }

    /**
     * 创建消息
     * @param string $msg 消息
     * @param int $time 延迟时间  需要安装扩展
     * @return object|AMQPMessage
     */
    private static function createMessageDelay(string $msg, int $time = 0): object
    {
        $delayConfig = [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT, // 持久化消息（避免重启丢失）
        ];
        // 延迟消息：添加x-delay头（需交换机类型为x-delayed-message）
        if ($time > 0) {
            $delayConfig['application_headers'] = new AMQPTable(['x-delay' => $time * 1000]);
        }
        return new AMQPMessage($msg, $delayConfig);
    }

    /**
     * 投递消息
     * @param string $msg 消息（JSON字符串）
     * @param int $delay 延迟时间（秒）
     * @return bool 是否投递成功
     * @note 完善：使用当前队列的connectKey和信道，确保消息投递到正确队列
     */
    private static function sendDelay(string $msg, int $delay = 0)
    {
        // 1. 获取当前队列的唯一标识
        [$exchangeName, $queueName, $connectKey] = static::getQueueName();

        // 2. 确保连接有效
        if (!static::isConnected($connectKey)) {
            if (!static::make()) {
                static::error(new \RuntimeException("队列[{$queueName}]连接无效，投递失败"));
                return false;
            }
        }

        // 3. 获取当前队列的信道
        $channel = static::$channels[$connectKey];
        if (!$channel || !$channel->is_open()) {
            static::error(new \RuntimeException("队列[{$queueName}]信道已关闭，投递失败"));
            static::close($connectKey); // 清理无效信道
            return false;
        }

        // 4. 创建消息（持久化+延迟配置）
        static::$timeOut = $delay;
        $_msg = static::createMessageDelay($msg, static::$timeOut);

        try {
            // 5. 投递消息到当前队列的交换机+路由键
            $channel->basic_publish(
                $_msg,
                $exchangeName,  // 动态生成的交换机名（非static::$exchangeName）
                $queueName      // 路由键=队列名（确保精准投递）
            );
            // static::error(new \RuntimeException("队列[{$queueName}]消息投递成功：" . $msg));
            return true;
        } catch (\Exception $exception) {
            // 6. 投递失败：清理资源，返回失败
            static::error(new \RuntimeException("队列[{$queueName}]消息投递失败：" . $exception->getMessage() . "，消息：" . $msg));
            static::close($connectKey);
            return false;
        }
    }

    /**
     * 关闭指定队列的连接和信道
     * @param string $key 连接key（交换机名@队列名）
     * @return void
     * @note 完善：仅清理指定key的资源，不影响其他队列
     */
    public static function close(string $key)
    {
        // 1. 清理信道（先关信道再关连接）
        if (isset(static::$channels[$key]) && static::$channels[$key]->is_open()) {
            try {
                static::$channels[$key]->close();
            } catch (\Exception $e) {
                // 忽略关闭异常（避免资源已释放导致报错）
            }
            unset(static::$channels[$key]); // 从数组中移除
        }

        // 2. 清理连接
        if (isset(static::$connections[$key]) && static::$connections[$key]->isConnected()) {
            try {
                static::$connections[$key]->close();
            } catch (\Exception $e) {
                // 忽略关闭异常
            }
            unset(static::$connections[$key]); // 从数组中移除
        }

        // 3. 清理连接状态
        if (isset(static::$connecting[$key])) {
            static::$connecting[$key] = false;
        }
    }

    /**
     * 判断指定队列的连接是否有效
     * @param string $key 连接key（交换机名@队列名）
     * @return bool 连接+信道是否均有效
     * @note 完善：仅判断指定key的资源，避免全局属性干扰
     */
    protected static function isConnected(string $key)
    {
        // 1. 检查连接是否存在且已连接
        if (!isset(static::$connections[$key]) || !static::$connections[$key]->isConnected()) {
            return false;
        }

        // 2. 检查信道是否存在且已打开
        if (!isset(static::$channels[$key]) || !static::$channels[$key]->is_open()) {
            return false;
        }

        // 3. 连接+信道均有效
        return true;
    }

    /**
     * 获取休眠时间
     * @return float|int
     * @note 随着失败次数增加，休眠时间呈指数递增，目的是为了实现消费者的自我维护
     */
    private static function getSleepTime()
    {
        /** 最大休眠60秒，确保不会频繁请求服务端造成资源浪费 */
        return \min(static::$hasRetryConnect * 5 + \rand(0, 5), 60);
    }

    /**
     * 投递消息（对外接口）
     * @param array $msg 消息内容（关联数组）
     * @param int $time 延迟时间（秒）
     * @return bool 是否投递成功
     */
    public static function publish(array $msg, int $time = 0)
    {
        // 投递时关闭重连（避免投递逻辑被重连阻塞）
        static::$maxRetryConnect = 0;
        // 转JSON字符串（保留中文）
        $jsonMsg = json_encode($msg, JSON_UNESCAPED_UNICODE);
        if ($jsonMsg === false) {
            static::error(new \RuntimeException("消息JSON编码失败：" . json_last_error_msg()));
            return false;
        }
        return static::sendDelay($jsonMsg, $time);
    }
}