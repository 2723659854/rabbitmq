<?php

namespace Xiaosongshu\Rabbitmq;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;


abstract class Client2 implements RabbiMQInterface
{
    /** 原有属性保留 */
    public static $host = "";
    public static $port = 5672;
    public static $user = "guest";
    public static $pass = "guest";
    protected static $channel;
    protected static $connection;
    public static $timeOut = 0;
    public static $exchangeName = "";
    public static $queueName = "";
    public static $type = "";
    const EXCHANGETYPE_FANOUT = "fanout";
    const EXCHANGETYPE_DIRECT = "direct";
    const EXCHANGETYPE_DELAYED = "x-delayed-message";
    const ACK = 1;
    const NACK = 2;
    const REJECT = 3;
    protected static $instance = null;
    protected static $maxRetryConnect = 5;
    protected static $hasRetryConnect = 0;
    public static $heartbeat = 60;
    protected static $total = 0;
    protected static $remain = 0;


    /**
     * 死信队列相关配置
     */
    public static $enableDlx = false;
    public static $dlxExchangeName = "";
    public static $dlxQueueName = "";
    public static $dlxRoutingKey = "";
    public static $dlxMessageTtl = 0;
    protected static $dlxPid = 0;

    protected static $IS_NOT_WINDOWS = false;

    /**
     * 构建队列
     * @return bool
     */
    private static function make()
    {
        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN')
        {
            static::$IS_NOT_WINDOWS = true;
        }
        /** 如果已连接服务端，则不要重新连接 */
        if (static::isConnected()) {
            return true;
        }

        /** 正在连接中，不要重复连接 */
        static $connecting = false;
        if ($connecting) {
            return true;
        }

        $connecting = true;

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
            // 1. 创建新连接和通道
            static::$connection = new AMQPStreamConnection(
                static::$host, static::$port, static::$user, static::$pass,
                '/', false, 'AMQPLAIN', null, 'en_US', 1.0,
                3.0, null, false, static::$heartbeat, 0.0, null, null
            );

            static::$channel = static::$connection->channel();

            // 2. 声明业务交换机
            static::$channel->exchange_declare(
                static::$exchangeName,
                static::$type ?: static::EXCHANGETYPE_DIRECT,
                false,
                true,
                false
            );

            // 3. 准备队列参数
            $queueArguments = new AMQPTable();
            if (static::$enableDlx) {
                $queueArguments->set('x-dead-letter-exchange', static::$dlxExchangeName);
                $queueArguments->set('x-dead-letter-routing-key', static::$dlxRoutingKey);
                $queueArguments->set('x-message-ttl', 3);
            }

            // 4. 处理业务队列（存在即使用，不存在则创建）
            try {
                // 先尝试被动声明，检查队列是否存在
                static::$channel->queue_declare(
                    static::$queueName,
                    true,  // passive=true
                    true,  // durable
                    false,
                    false
                );
                //echo "业务队列已存在，直接使用: " . static::$queueName . PHP_EOL;
            } catch (AMQPProtocolChannelException $e) {
                if ($e->getCode() == 404) {
                    // 队列不存在，创建新队列
                    static::$channel->queue_declare(
                        static::$queueName,
                        false,
                        true,
                        false,
                        false,
                        false,
                        $queueArguments
                    );
                    //echo "业务队列创建成功: " . static::$queueName . PHP_EOL;
                } else {
                    throw $e;
                }
            }

            // 绑定业务队列到交换机（无论队列是否已存在都需要绑定）
            static::$channel->queue_bind(
                static::$queueName,
                static::$exchangeName,
                static::$queueName
            );

            // 5. 处理死信队列（仅在启用时）
            if (static::$enableDlx) {
                // 创建新的通道专门用于死信队列操作
                $dlxChannel = static::$connection->channel();

                try {
                    // 声明死信交换机
                    $dlxChannel->exchange_declare(
                        static::$dlxExchangeName,
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

                    // 处理死信队列（存在即使用，不存在则创建）
                    try {
                        $dlxChannel->queue_declare(
                            static::$dlxQueueName,
                            true,  // passive=true
                            true,  // durable
                            false,
                            false
                        );
                        //echo "死信队列已存在，直接使用: " . static::$dlxQueueName . PHP_EOL;
                    } catch (AMQPProtocolChannelException $e) {
                        if ($e->getCode() == 404) {
                            // 死信队列不存在，创建新队列
                            $dlxChannel->queue_declare(
                                static::$dlxQueueName,
                                false,
                                true,
                                false,
                                false,
                                false,
                                $dlxQueueArguments
                            );
                            //echo "死信队列创建成功: " . static::$dlxQueueName . PHP_EOL;
                        } else {
                            throw $e;
                        }
                    }

                    // 绑定死信队列（无论队列是否已存在都需要绑定）
                    $dlxChannel->queue_bind(
                        static::$dlxQueueName,
                        static::$dlxExchangeName,
                        static::$dlxRoutingKey
                    );
                } finally {
                    // 关闭死信专用通道
                    if ($dlxChannel->is_open()) {
                        $dlxChannel->close();
                    }
                }
            }

            static::$instance = static::$connection;
            static::$hasRetryConnect = 0;
            $connecting = false;
            return true;

        } catch (\Exception $exception) {
            $connecting = false;
            static::close();
            if (static::$hasRetryConnect < static::$maxRetryConnect) {
                static::$hasRetryConnect++;
                call_user_func([get_called_class(), 'error'], new \RuntimeException(
                    json_encode([
                        'message' => "尝试重连RabbitMQ（" . static::$hasRetryConnect . "次）：" . $exception->getMessage()
                    ], JSON_UNESCAPED_UNICODE)
                ));
                sleep(static::getSleepTime());
                return static::make();
            } else {
                call_user_func([get_called_class(), 'error'], new \RuntimeException(
                    json_encode([
                        'message' => "RabbitMQ连接失败：" . $exception->getMessage()
                    ], JSON_UNESCAPED_UNICODE)
                ));
                return false;
            }
        }
    }

    /**
     * 开启死信队列消费
     * @return void
     * @note 主要适用于于windows
     */
    public static function consumeDead()
    {
        static::startDlxConsumer();
    }

    /**
     * 开启死信队列消费
     * @return void
     */
    private static function startDlxConsumer()
    {
        if (!static::$enableDlx) {
            return;
        }

        // Windows 系统处理
        if (!static::$IS_NOT_WINDOWS) {
            // 在Windows下直接启动消费循环，不创建子进程
            static::runDlxConsumer();
            return;
        }

        // Linux 系统处理
        if (static::$dlxPid > 0 && \posix_kill(static::$dlxPid, 0)) {
            return;
        }

        $pid = \pcntl_fork();
        if ($pid == -1) {
            call_user_func([get_called_class(), 'error'], new \RuntimeException(
                json_encode(['message' => "创建死信消费子进程失败"], JSON_UNESCAPED_UNICODE)
            ));
            return;
        } elseif ($pid == 0) {
            static::runDlxConsumer();
            exit(0);
        } else {
            static::$dlxPid = $pid;
        }
    }

    /**
     * 执行死信队列消费逻辑（公共方法，Windows/Linux共用）
     * @return void
     */
    private static function runDlxConsumer()
    {
        while (true) {
            if (!static::isConnected()) {
                static::make();
                if (!static::isConnected()) {
                    sleep(3);
                    continue;
                }
            }

            try {
                $dlxCallback = function ($msg) {
                    $dlxMsg = json_decode($msg->body, true);

                    $class = get_called_class();
                    try {
                        if (method_exists($class, 'dlxHandle')) {
                            $result = call_user_func([$class, 'dlxHandle'], $dlxMsg);
                            if ($result === static::ACK) {
                                static::$channel->basic_ack($msg->delivery_info['delivery_tag'], false);
                            } else {
                                static::$channel->basic_nack($msg->delivery_info['delivery_tag'], false, false);
                                call_user_func([get_called_class(), 'error'], new \RuntimeException(
                                    json_encode(['message' => "死信消息处理失败"], JSON_UNESCAPED_UNICODE)
                                ));
                            }
                        } else {
                            static::$channel->basic_ack($msg->delivery_info['delivery_tag'], false);
                            call_user_func([get_called_class(), 'error'], new \RuntimeException(
                                json_encode(['message' => "未实现dlxHandle，自动确认死信"], JSON_UNESCAPED_UNICODE)
                            ));
                        }
                    } catch (\Exception $e) {
                        static::$channel->basic_nack($msg->delivery_info['delivery_tag'], false, false);
                        call_user_func([get_called_class(), 'error'], new \RuntimeException(
                            json_encode([
                                'message' => "死信处理异常：" . $e->getMessage()
                            ], JSON_UNESCAPED_UNICODE)
                        ));
                    }
                };

                static::$channel->basic_qos(0, 1, false);
                static::$channel->basic_consume(
                    static::$dlxQueueName,
                    static::$dlxQueueName . "_consumer",
                    false,
                    false,
                    false,
                    false,
                    $dlxCallback
                );

                while (count(static::$channel->callbacks)) {
                    static::$channel->wait();
                }
            } catch (\Exception $e) {
                call_user_func([get_called_class(), 'error'], new \RuntimeException(
                    json_encode([
                        'message' => "死信消费异常：" . $e->getMessage()
                    ], JSON_UNESCAPED_UNICODE)
                ));
                static::close();
                sleep(3);
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
            call_user_func([get_called_class(), 'error'], new \RuntimeException(
                json_encode([
                    'message' => "死信队列子进程（" . static::$dlxPid . "）已退出，准备重启"
                ], JSON_UNESCAPED_UNICODE)
            ));
            static::$dlxPid = 0;
            static::startDlxConsumer();
        }
    }

    /**
     * 开启消费
     * @param int $count
     * @return void
     */
    public static function consume(int $count = 0)
    {
        static::$total = static::$remain = $count;


        if (static::$enableDlx) {
            /** 非windows系统 开启子进程消费死信队列数据 */
            if (static::$IS_NOT_WINDOWS) {
                static::startDlxConsumer();
            }
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
                            call_user_func([get_called_class(), 'error'], new \RuntimeException(
                                json_encode([
                                    'message' => "未实现handle方法，拒绝消息"
                                ], JSON_UNESCAPED_UNICODE)
                            ));
                        }
                    } catch (\Exception $exception) {
                        static::$channel->basic_reject($msg->delivery_info['delivery_tag'], false);
                        call_user_func([get_called_class(), 'error'], new \RuntimeException(
                            json_encode([
                                'message' => "业务消费异常：" . $exception->getMessage()
                            ], JSON_UNESCAPED_UNICODE)
                        ));
                    }
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
                        if (static::$IS_NOT_WINDOWS){
                            static::monitorDlxProcess();
                        }
                        static::$channel->wait();
                    }
                } else {
                    /** 如果没有限制，那么就一直消费 */
                    while (count(static::$channel->callbacks)) {
                        if (static::$IS_NOT_WINDOWS){
                            static::monitorDlxProcess();
                        }
                        static::$channel->wait();
                    }
                }
                break;
            } catch (\PhpAmqpLib\Exception\AMQPConnectionClosedException $exception) {
                call_user_func([get_called_class(), 'error'], new \RuntimeException(
                    json_encode([
                        'message' => "业务消费连接断开：" . $exception->getMessage()
                    ], JSON_UNESCAPED_UNICODE)
                ));
                static::close();
                sleep(static::getSleepTime());
            }
        }

        if (static::$enableDlx && static::$dlxPid > 0) {
            \posix_kill(static::$dlxPid, SIGTERM);
            call_user_func([get_called_class(), 'error'], new \RuntimeException(
                json_encode([
                    'message' => "关闭死信子进程（PID：" . static::$dlxPid . "）"
                ], JSON_UNESCAPED_UNICODE)
            ));
        }
        static::close();
    }

    /**
     * 创建消息
     * @param string $msg
     * @param int $time
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
     * @param string $msg
     * @param int $delay
     * @return bool
     */
    private static function sendDelay(string $msg, int $delay = 0)
    {
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
                static::$channel->close();
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
        return (static::$connection && static::$connection->isConnected()
            && static::$channel && static::$channel->is_open());
    }

    /**
     * 获取休眠时间
     * @return float|int
     */
    private static function getSleepTime()
    {
        return static::$hasRetryConnect * 5 + rand(0, 5);
    }

    /**
     * 投递消息
     * @param array $msg
     * @param int $time
     * @return bool
     */
    public static function publish(array $msg, int $time = 0)
    {
        static::$maxRetryConnect = 0;
        return static::sendDelay(json_encode($msg, JSON_UNESCAPED_UNICODE), $time);
    }

}