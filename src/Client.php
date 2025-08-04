<?php

namespace Xiaosongshu\Rabbitmq;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;

/**
 * @purpose rabbitmq客户端
 * @author yanglong
 * @time 2025年7月23日14:07:46
 * @note 重构新版本队列客户端，主要是为了解决客户端因为业务卡死而掉线的问题。
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
    /** @var \PhpAmqpLib\Channel\AbstractChannel|\PhpAmqpLib\Channel\AMQPChannel 渠道通道 */
    protected static $channel;
    /** @var AMQPStreamConnection rabbitmq连接 */
    protected static $connection;

    /** @var int 过期时间 */
    public static $timeOut = 0;
    /** @var string 交换机名称 */
    public static $exchangeName = "";
    /** @var string 队列名称 */
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
    /** rabbitmq链接 */
    protected static $instance = null;

    /** 重新连接最大尝试次数，可能rabbitmq服务网络跳动等异常情况，所以设置大一点 */
    protected static $maxRetryConnect = 50;

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
    /** 死信队列交换机 */
    public static $dlxExchangeName = "";
    /** 死信队列名称 */
    public static $dlxQueueName = "";
    /** 死信队列路由 */
    public static $dlxRoutingKey = "";
    /** 死信消息生命周期 */
    public static $dlxMessageTtl = 0;
    /** 死信队列进程id */
    protected static $dlxPid = 0;
    /** 操作系统不是windows */
    protected static $IS_NOT_WINDOWS = false;

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
     * 连接服务器并创建队列
     * @return bool
     */
    private static function make()
    {
        /** 检测操作系统 */
        static::isNotWindows();

        /** 如果已连接服务端，则不要重新连接 */
        if (static::isConnected()) {
            return true;
        }

        /** 正在连接中，不要重复连接 */
        static $connecting = false;
        if ($connecting) {
            return true;
        }
        /** 标记当前正在连接中 */
        $connecting = true;

        /** 先强制关闭所有连接，清理资源 */
        static::close();
        if (!static::$type) {
            static::$type = static::EXCHANGETYPE_DIRECT;
        }
        $className = get_called_class();
        if (!static::$exchangeName) {
            static::$exchangeName = $className;
        }
        if (!static::$queueName) {
            static::$queueName = $className;
        }

        /** 如果开启了信心队列，那么其相关配置如下 */
        if (static::$enableDlx) {
            if (!static::$dlxExchangeName) {
                static::$dlxExchangeName = static::$exchangeName . "_dlx";
            }
            if (!static::$dlxQueueName) {
                static::$dlxQueueName = static::$queueName . "_dlx";
            }
            if (!static::$dlxRoutingKey) {
                static::$dlxRoutingKey = static::$queueName . "_dlx";
            }
        }

        try {
            /** 创建新连接和通道，默认连接超时1秒，读写超时3秒，心跳间隔最大60秒 */
            static::$connection = new AMQPStreamConnection(
                static::$host, static::$port, static::$user, static::$pass,
                '/', false, 'AMQPLAIN', null, 'en_US', 1.0,
                3.0, null, false, static::$heartbeat, 0.0, null, null
            );
            /** 获取信道 */
            static::$channel = static::$connection->channel();

            /** 声明业务交换机 */
            static::$channel->exchange_declare(
                static::$exchangeName,
                static::$type ?: static::EXCHANGETYPE_DIRECT,
                false,
                true,
                false
            );

            /** 准备队列参数 ，普通的业务队列的相关参数 */
            $queueArguments = new AMQPTable();
            if (static::$enableDlx) {
                $queueArguments->set('x-dead-letter-exchange', static::$dlxExchangeName);
                $queueArguments->set('x-dead-letter-routing-key', static::$dlxRoutingKey);
                /** 此处定义普通消息的生命周期为30秒 ，30秒足够队列来处理了，如果时间过长，那么就应该考虑使用脚本来处理，而不是队列 */
                $queueArguments->set('x-message-ttl', 30000);
            }

            try {
                // 直接主动创建队列（确保队列被创建）
                static::$channel->queue_declare(
                    static::$queueName,
                    false, // passive=false（主动创建）
                    true,
                    false,
                    false,
                    false,
                    $queueArguments
                );
                /** 绑定业务队列到交换机（无论队列是否已存在都需要绑定） */
                static::$channel->queue_bind(
                    static::$queueName,
                    static::$exchangeName,
                    static::$queueName
                );
            } catch (\Exception|\Throwable $exception) {
                /** 发生了异常，通知用户立即处理 ，只提示错误，不提任何建议，由用户自己斟酌处理 */
                call_user_func([get_called_class(), 'error'], new \RuntimeException($exception->getMessage()));
            }


            /** 处理死信队列（仅在启用时），死信队列和业务队列使用不同的信道，这里是为了防止逻辑混乱，防止排队等待，实际可以共用一个信道 */
            if (static::$enableDlx) {
                /** 创建新的通道专门用于死信队列操作 */
                $dlxChannel = static::$connection->channel();

                try {

                    /** 声明死信交换机 */
                    $dlxChannel->exchange_declare(
                        static::$dlxExchangeName,
                        static::EXCHANGETYPE_DIRECT,
                        false,
                        true,
                        false
                    );

                    /** 准备死信队列参数 */
                    $dlxQueueArguments = new AMQPTable();
                    if (static::$dlxMessageTtl > 0) {
                        /** 设置死信队列消息的有效期 */
                        $dlxQueueArguments->set('x-message-ttl', static::$dlxMessageTtl);
                    }

                    /** 死信队列不存在，创建新队列 */
                    $dlxChannel->queue_declare(
                        static::$dlxQueueName,
                        false,
                        true,
                        false,
                        false,
                        false,
                        $dlxQueueArguments
                    );

                    /** 绑定死信队列（无论队列是否已存在都需要绑定） */
                    $dlxChannel->queue_bind(
                        static::$dlxQueueName,
                        static::$dlxExchangeName,
                        static::$dlxRoutingKey
                    );
                } catch (\Exception|\Throwable $exception) {
                    /** 发生了异常，通知用户立即处理 ，只提示错误，不提任何建议，由用户自己斟酌处理 */
                    call_user_func([get_called_class(), 'error'], new \RuntimeException($exception->getMessage()));
                } finally {
                    /** 申明死信队列后，因为死信队列的消费是另外一个进程，所以当前信道不再使用，为了防止资源浪费，则关闭死信队列的信道，而业务队列的信道需要立刻使用，比如投递或者消费，所以不能关闭 */
                    if ($dlxChannel->is_open()) {
                        $dlxChannel->close();
                    }
                }
            }
            /** 保存连接信息 */
            static::$instance = static::$connection;
            static::$hasRetryConnect = 0;
            $connecting = false;
            return true;

        } catch (\Exception $exception) {
            /** 自动重连 */
            $connecting = false;
            /** 清理原有的资源 */
            static::close();

            /** 如果设置了最大重连次数，那么就一直重连，永不退出，目的是为了客户端自我维护，自动登录，防止因为卡死掉线而不能及时处理消息，如果服务端挂了，则需要手动停止消费者 */
            if (static::$maxRetryConnect) {
                static::$hasRetryConnect++;
                call_user_func([get_called_class(), 'error'], new \RuntimeException("尝试重连RabbitMQ（" . static::$hasRetryConnect . "次）"));
                sleep(static::getSleepTime());
                return static::make();
            } else {
                /** 尝试完毕，仍无法连接，退出 */
                call_user_func([get_called_class(), 'error'], new \RuntimeException("RabbitMQ连接失败"));
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
     */
    private static function startDlxConsumer()
    {
        /** 不开启死信队列则不处理 */
        if (!static::$enableDlx) {
            return;
        }

        // Windows 系统处理
        if (!static::$IS_NOT_WINDOWS) {
            /** 在Windows下直接启动消费循环，不创建子进程 */
            static::runDlxConsumer();
            return;
        }

        // Linux 系统处理
        /** 如果存在死信进程，并且拥有对子进程的控制权，那么 就返回，这里发送的是0只用于检查，不是9杀死 */
        if (static::$dlxPid > 0 && \posix_kill(static::$dlxPid, 0)) {
            return;
        }
        /** 处理队列名称为空的场景 */
        if (empty(static::$queueName)){
            static::$queueName = get_called_class();
        }
        /** 创建子进程用于死信队列，防止普通业务队列阻塞影响死信队列 */
        $pid = \pcntl_fork();
        if ($pid == -1) {
            call_user_func([get_called_class(), 'error'], new \RuntimeException("创建死信消费子进程失败"));
            return;
        } elseif ($pid == 0) {
            cli_set_process_title(static::$queueName."_dlx");
            /** 子进程 负责死信队列 消费 */
            static::runDlxConsumer();
            exit(0);
        } else {
            /** 主进程记录死信队列的进程id */
            static::$dlxPid = $pid;
            cli_set_process_title(static::$queueName);
        }
    }


    /**
     * 死信队列消费者逻辑
     * @return void
     * @note 自动重连，不退出，属于兜底操作
     */
    private static function runDlxConsumer()
    {
        $retryCount = 0;
        $maxRetry = 5; // 最大连续重试次数
        $backoffTime = 3; // 初始退避时间（秒）
        while (true) {
            try {
                // 检查连接，未连接则重建（确保每次重试都是全新连接）
                if (!static::isConnected()) {
                    // 强制关闭可能残留的旧连接
                    static::close();
                    static::make();

                    if (!static::isConnected()) {
                        throw new \RuntimeException("连接建立失败，将重试");
                    }

                    // 重置重试计数（连接成功则清零）
                    $retryCount = 0;
                    $backoffTime = 3;
                }

                /** 取消死信队列的已注册的消费者 */
                $dlxConsumerPrefix = static::$dlxQueueName . "_consumer_";
                foreach (static::$channel->callbacks as $consumerTag => $_) {
                    // 只取消死信队列的消费者（标签以死信前缀开头）
                    if (strpos($consumerTag, $dlxConsumerPrefix) === 0) {
                        static::$channel->basic_cancel($consumerTag, true);
                        // 从本地回调列表中移除（同步状态）
                        unset(static::$channel->callbacks[$consumerTag]);
                    }
                }

                /** 构建死信队列消费者逻辑 */
                $dlxCallback = function ($msg) {
                    $dlxMsg = json_decode($msg->body, true);
                    $class = get_called_class();

                    try {
                        if (!method_exists($class, 'dlxHandle')) {
                            static::$channel->basic_ack($msg->delivery_info['delivery_tag'], false);
                            call_user_func([$class, 'error'], new \RuntimeException("未实现dlxHandle，自动确认死信" . " [消息体]: " . $msg->body));
                            return;
                        }
                        /** 处理异常业务逻辑 */
                        $result = call_user_func([$class, 'dlxHandle'], $dlxMsg);
                        if ($result === static::ACK) {
                            static::$channel->basic_ack($msg->delivery_info['delivery_tag'], false);
                        } else {
                            /** 消费失败则重新入队，交给下一个死信消费者处理 */
                            static::$channel->basic_nack($msg->delivery_info['delivery_tag'], false, true);
                            call_user_func([$class, 'error'], new \RuntimeException("死信消息处理失败" . " [消息体]: " . $msg->body));
                        }
                    } catch (\Throwable $e) {
                        /** 方法不存在，代码有错误等等异常行为，已经引起消费者崩溃了，那么就没有必要再次投递，赶紧修改代码吧 */
                        static::$channel->basic_nack($msg->delivery_info['delivery_tag'], false, false);
                        call_user_func([$class, 'error'], new \RuntimeException("死信处理异常：" . $e->getMessage() . " [消息体]: " . $msg->body));
                    }
                };

                /** 这里复用了通用业务队列的信道，原因：信道只负责传输数据 */
                // 重置消费者配置（确保每次重建都是全新的消费者）
                static::$channel->basic_qos(0, 1, false);
                /** 同一个信道上数据的区分是 依靠队列实现的，同一个信道上可以传输多个队列的数据 */
                /** 这里给信道绑定了队列以及消费者 */
                static::$channel->basic_consume(
                    static::$dlxQueueName,
                    static::$dlxQueueName . "_consumer_" . uniqid(), // 消费者标签加唯一ID，避免冲突
                    false,
                    false,
                    false,
                    false,
                    $dlxCallback
                );

                while (count(static::$channel->callbacks)) {
                    /** 一直监听等待消息，永不退出 */
                    static::$channel->wait();
                    /** 切换cpu */
                    usleep(1);
                }


            } catch (\Exception $e) {
                $retryCount++;
                $errorMsg = "死信消费异常（第{$retryCount}次重试）：" . $e->getMessage();
                call_user_func([get_called_class(), 'error'], new \RuntimeException($errorMsg));
                // 关闭当前连接（无论是否成功，强制清理）
                static::close();
                // 退避策略：连续失败次数越多，等待时间越长（最多等待30秒）
                if ($retryCount >= $maxRetry) {
                    $backoffTime = min($backoffTime * 2, 30);
                }
                sleep($backoffTime);
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
        $result = \pcntl_waitpid(static::$dlxPid, $status, WNOHANG);
        if ($result == static::$dlxPid) {
            call_user_func([get_called_class(), 'error'], new \RuntimeException("死信队列子进程（" . static::$dlxPid . "）已退出，准备重启"));
            static::$dlxPid = 0;
            static::startDlxConsumer();
        }
    }

    /**
     * 开启消费
     * @param int $count 指定本次消费数量
     * @return void
     */
    public static function consume(int $count = 0)
    {
        /** 如果指定了本次消费消息数量后退出，则初始化总量和剩余可消费量 */
        static::$total = static::$remain = $count;
        static::isNotWindows();
        if (static::$enableDlx) {
            /** 非windows系统 开启子进程消费死信队列数据 */
            if (static::$IS_NOT_WINDOWS) {
                static::startDlxConsumer();
            }
        }
        /** 主进程设置名称 */
        if (empty(static::$queueName)){
            static::$queueName = get_called_class();
        }
        /** 非windows系统才设置主进程名称 */
        if (static::$IS_NOT_WINDOWS){
            @cli_set_process_title(static::$queueName);
        }
        while (true) {

            /** 非windows系统才监控死信队列 */
            if (static::$enableDlx && static::$IS_NOT_WINDOWS) {
                static::monitorDlxProcess();
            }

            /** 判断是否连接可用 */
            if (!static::isConnected()) {
                static::make();
                if (!static::isConnected()) {
                    sleep(3);
                    continue;
                }
            }

            try {
                /** 构建回调函数 */
                $businessCallback = function ($msg) {
                    /** 数据解码 */
                    $params = json_decode($msg->body, true);
                    try {
                        $class = get_called_class();
                        if (class_exists($class) && method_exists($class, 'handle')) {
                            /** 执行业务逻辑 */
                            $ack = call_user_func([$class, 'handle'], $params);
                            /** 成功 确认ok */
                            if ($ack == static::ACK) {
                                static::$channel->basic_ack($msg->delivery_info['delivery_tag'], false);
                            }
                            /** 失败 重新投递 */
                            if ($ack == static::NACK) {
                                static::$channel->basic_nack($msg->delivery_info['delivery_tag'], false, true);
                            }
                            /** 拒绝消息，重新投递 ，在业务依赖的其他服务不可用等场景的时候拒绝消息，消息就会重新投递分配给其他消费者处理 */
                            if ($ack == static::REJECT) {
                                static::$channel->basic_reject($msg->delivery_info['delivery_tag'], true);
                            }
                        } else {
                            /** 核心业务逻辑方法不存在 拒绝，但不重新投递，因为没有消费方法，投递也是浪费资源，不能解决根本问题 */
                            static::$channel->basic_reject($msg->delivery_info['delivery_tag'], false);
                            call_user_func([get_called_class(), 'error'], new \RuntimeException("未实现handle方法，拒绝消息" . " [消息体]: " . $msg->body));
                        }
                    } catch (\Throwable  $exception) {# 这里使用Throwable囊括所有的php异常和错误，防止进程中断
                        /** 因为代码错误导致程序崩溃，拒绝再次投递，以免引起其他消费者崩溃。赶紧修改代码吧 */
                        static::$channel->basic_reject($msg->delivery_info['delivery_tag'], false);
                        call_user_func([get_called_class(), 'error'], new \RuntimeException("业务消费异常：" . $exception->getMessage() . " [消息体]: " . $msg->body));
                    }
                    /** 如果执行了消费数量，那么需要减少数量 */
                    if (static::$total > 0) {
                        static::$remain--;
                    }
                };

                /** 一次一条消息，在未收到消息确认之前不会分配新的消息 */
                static::$channel->basic_qos(0, 1, false);
                /** 给队列绑定消费逻辑 这里要注意一下，第二个参数添加了标签，主要是用来后面关闭通道使用，并且不会接收本消费者发送的消息 */
                static::$channel->basic_consume(
                    static::$queueName,
                    static::$queueName,
                    false,
                    false,
                    false,
                    false,
                    $businessCallback
                );

                /** 如果设置了消费总数限制 */
                if (static::$total) {
                    /** 没次消费都要判断剩余可消费数 */
                    while (static::$remain > 0 && count(static::$channel->callbacks)) {
                        if (static::$IS_NOT_WINDOWS) {
                            static::monitorDlxProcess();
                        }
                        static::$channel->wait();
                        /** 切换cpu */
                        usleep(1);
                    }
                } else {
                    /** 如果没有限制，那么就一直消费 */
                    # static::$channel->callbacks 获取当前信道上已注册的消费者
                    // 如果还有存活的已注册的消费者，那么就要继续等待消息，如果消费者 被手动取消basic_cancel，或者被服务器连接断开，消费者被清空
                    // 那么这个时候就不需要等待接受新消息，退出监听
                    while (count(static::$channel->callbacks)) {
                        if (static::$IS_NOT_WINDOWS) {
                            static::monitorDlxProcess();
                        }
                        static::$channel->wait();
                        /** 切换cpu */
                        usleep(1);
                    }
                }
                /** 消费完成，则退出循环 */
                break;
            } catch (\PhpAmqpLib\Exception\AMQPConnectionClosedException $exception) {
                /** 连接断开则重连 */
                call_user_func([get_called_class(), 'error'], new \RuntimeException("业务消费连接断开：" . $exception->getMessage()));
                static::close();
                sleep(static::getSleepTime());
            } catch (\Throwable $exception) {
                /** 其他类型的异常，先处理错误 */
                call_user_func([get_called_class(), 'error'], new \RuntimeException($exception->getMessage()));

                // 默认这些错误不可恢复，比如代码错误，需要修复代码
                $isPossiblyRecoverable = false;
                // 但是如果这些错误是rabbitmq的，那么可能是服务信号异常等情况，属于可恢复的条件
                if ($exception instanceof \PhpAmqpLib\Exception\AMQPRuntimeException) {
                    // RabbitMQ相关异常：部分是临时的（如信道满了），这种等一下可以恢复
                    $isPossiblyRecoverable = true;
                }

                // 确认不能恢复，则断开连接
                if (!$isPossiblyRecoverable) {
                    call_user_func([get_called_class(), 'error'], new \RuntimeException("判断为致命错误（非临时异常），进程退出。详情：" . $exception->getMessage()));
                    break;
                }

                // 临时错误：继续重连
                static::close();
                sleep(static::getSleepTime());
            }
        }

        if (static::$enableDlx && static::$dlxPid > 0) {
            /** linux环境自动杀死子进程，windows环境是独立的进程不需要杀死 */
            if (static::isNotWindows()) {
                \posix_kill(static::$dlxPid, \SIGTERM);
                call_user_func([get_called_class(), 'error'], new \RuntimeException("关闭死信子进程（PID：" . static::$dlxPid . "）"));
            }
        }
        /** 回收连接 */
        static::close();
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
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];
        if ($time) {
            $delayConfig['application_headers'] = new AMQPTable(['x-delay' => $time * 1000]);
        }
        return new AMQPMessage($msg, $delayConfig);
    }

    /**
     * 投递消息
     * @param string $msg 消息
     * @param int $delay 延迟时间
     * @return bool 是否投递成功
     */
    private static function sendDelay(string $msg, int $delay = 0)
    {
        /** 构建连接 */
        if (!static::isConnected()) {
            static::make();
            if (!self::isConnected()) {
                return false;
            }
        }
        static::$timeOut = $delay;
        $_msg = static::createMessageDelay($msg, static::$timeOut);
        try {
            static::$channel->basic_publish($_msg, static::$exchangeName, static::$queueName);
            return true;
        } catch (\Exception $exception) {
            return false;
        }
    }

    /**
     * 关闭服务
     * @return void
     */
    public static function close()
    {
        if (static::$instance && static::$instance->isConnected()) {
            try {
                /** 关闭信道 */
                static::$channel->close();
                /** 关闭连接 */
                static::$connection->close();
            } catch (\Exception $e) {
                // 忽略关闭异常
            }
        }
        static::$instance = null;
        static::$connection = null;
        static::$channel = null;
    }

    /**
     * 判断服务是否已连接
     * @return bool
     */
    protected static function isConnected()
    {
        return (static::$connection && static::$connection->isConnected() && static::$channel && static::$channel->is_open());
    }

    /**
     * 获取休眠时间
     * @return float|int
     * @note 随着失败次数增加，休眠时间呈指数递增，目的是为了实现消费者的自我维护
     */
    private static function getSleepTime()
    {
        /** 最大休眠60秒，确保不会频繁请求服务端造成资源浪费 */
        return min(static::$hasRetryConnect * 5 + rand(0, 5), 60);
    }

    /**
     * 投递消息
     * @param array $msg 消息
     * @param int $time 延迟时间
     * @return bool
     */
    public static function publish(array $msg, int $time = 0)
    {
        static::$maxRetryConnect = 0;
        return static::sendDelay(json_encode($msg, JSON_UNESCAPED_UNICODE), $time);
    }

}