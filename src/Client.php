<?php

namespace Xiaosongshu\Rabbitmq;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * @purpose rabbitmq投递和消费
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

    /** 重新连接最大尝试次数 */
    protected static $maxRetryConnect = 5;

    /** 已经尝试重连次数 */
    protected static $hasRetryConnect = 0;

    /** 心跳间隔 */
    public static $heartbeat = 60;

    /**
     * 构建客户端链接
     * @return bool|void
     * @throws \Exception
     * @note true 表示已连接 false表示未连接
     */
    private static function make()
    {
        /** 防止重复连接 */
        if (static::isConnected()) {
            return true;
        }

        /** 加锁防止并发重连 */
        static $connecting = false;
        if ($connecting) {
            return true;
        }
        $connecting = true;

        static::close();
        /** 初始化订阅方式 */
        if (!static::$type) {
            static::$type = static::EXCHANGETYPE_DIRECT;
        }
        $className = get_called_class();
        /** 初始化交换机 */
        if (!static::$exchangeName) {
            static::$exchangeName = $className;
        }
        /** 初始化队列 */
        if (!static::$queueName) {
            static::$queueName = $className;
        }

        try {
            static::$connection = new AMQPStreamConnection(
                static::$host, static::$port, static::$user, static::$pass,
                '/', false, 'AMQPLAIN', null, 'en_US', 1.0,
                3.0, null, false, static::$heartbeat, 0.0, null, null
            );

            /** 创建通道和其他初始化 */
            static::$channel = static::$connection->channel();
            static::$channel->exchange_declare(static::$exchangeName, static::$type ?: static::EXCHANGETYPE_DIRECT, false, true, false, false, false, new AMQPTable(["x-delayed-type" => static::EXCHANGETYPE_DIRECT]));
            static::$channel->queue_declare(static::$queueName, false, true, false, false);
            static::$channel->queue_bind(static::$queueName, static::$exchangeName, static::$queueName);

            static::$instance = static::$connection;
            static::$hasRetryConnect = 0;
            $connecting = false;
            return true;

        } catch (\Exception|\RuntimeException|AMQPRuntimeException|AMQPConnectionBlockedException $exception) {
            $connecting = false;
            static::close();
            /** 尝试重连 */
            if (static::$hasRetryConnect < static::$maxRetryConnect) {
                static::$hasRetryConnect++;
                call_user_func([get_called_class(), 'error'], new \RuntimeException(json_encode(['message' =>"attempt to reconnect to the rabbitmq-server ".static::$hasRetryConnect]), JSON_UNESCAPED_UNICODE));
                sleep(static::getSleepTime());
                static::make();
            } else {
                /** 超过重连次数，说明服务器挂了，没有必要再尝试了，直接通知家属，拜拜了你呢 */
                call_user_func([get_called_class(), 'error'], new \RuntimeException(json_encode(['message' => "rabbitmq server has gone away"]), JSON_UNESCAPED_UNICODE));
                return false;
            }
        }
    }

    /**
     * 获取休眠时间
     * @return int
     */
    private static function getSleepTime()
    {
        /** 设计理念：每一次尝试重连间隔时间逐渐增长，最长25秒，防止同一时间所有消费者疯狂请求连接服务端 */
        return static::$hasRetryConnect * 5 + rand(0, 5);
    }

    /**
     * 创建延迟信息
     * @param string $msg 消息内容
     * @param int $time 延迟时间 必须安装延迟插件，否则不能使用
     * @return AMQPMessage 包装后的消息
     */
    private static function createMessageDelay(string $msg, int $time = 0): object
    {
        $delayConfig = [
            /** 传递模式   消息持久化 ，这一个配置是消费确认发送ack的根本原因*/
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];
        if ($time) {
            /** 消息表头 设置延迟时间  延迟可以精确到毫秒 */
            $delayConfig['application_headers'] = new AMQPTable(['x-delay' => $time * 1000]);
        }

        return new AMQPMessage($msg, $delayConfig);
    }

    /**
     * 投递消息
     * @param string $msg 消息体
     * @param int $delay 延迟时间
     * @return bool true表示投递成功 false表示投递失败
     * @throws \Exception
     */
    private static function sendDelay(string $msg, int $delay = 0)
    {
        /** 检查链接 */
        if (!static::isConnected()) {
            static::make();
            if (!self::isConnected()) {
                return false;
            }
        }
        static::$timeOut = $delay;
        /** @var AMQPMessage $_msg 创建rabbitmq的延迟消息 */
        $_msg = static::createMessageDelay($msg, static::$timeOut);
        try {
            /** 发布消息 语法：消息体，交换机，路由（这里作者简化了用的队列名称代理路由名称）*/
            static::$channel->basic_publish($_msg, static::$exchangeName, static::$queueName);
            return true;
        } catch (\Exception|\RuntimeException $exception) {
            return false;
        }
    }

    /**
     * 关闭服务
     * @return void
     * @throws \Exception
     */
    public static function close()
    {
        if (static::$instance && static::$instance->isConnected()) {
            /** 发布完成后关闭通道 */
            static::$channel->close();
            /** 发布完成后关闭连接 */
            static::$connection->close();
        }
        static::$instance = null;
        static::$connection = null;
        static::$channel = null;
    }

    /** 设置本次消费个数 */
    protected static $total = 0;

    /** 当前队列剩余消费个数 */
    protected static $remain = 0;

    /**
     * 当前连接是否存活
     * @return bool
     */
    protected static function isConnected()
    {
        /** 检查链接和通道 */
        if (static::$connection && static::$connection->isConnected()
            && static::$channel && static::$channel->is_open()) {
            return true;
        }
        return false;
    }

    /**
     * 消费延迟队列
     * @param int $count 消费条数
     * @return void
     * @throws \Exception
     */
    private static function consumeDelay()
    {
        /** 检查链接 */
        if (!static::isConnected()) {
            static::make();
            if (!static::isConnected()) {
                return; // 重连失败直接返回
            }
        }

        /**
         * 创建一个回调函数，用来处理接收到的消息
         * @param $msg
         * @return void
         */
        $function = function ($msg) {
            $params = json_decode($msg->body, true);
            try {
                /** 调用用户的逻辑 */
                $class = get_called_class();
                if (class_exists($class) && method_exists($class, 'handle')) {
                    /** 处理业务逻辑 */
                    $ack = call_user_func([$class, 'handle'], $params);
                    if ($ack == static::ACK) {
                        /** 确认接收到消息 */
                        static::$channel->basic_ack($msg->delivery_info['delivery_tag'], false);
                    }
                    if ($ack == static::NACK) {
                        /** 重复投递 */
                        static::$channel->basic_nack($msg->delivery_info['delivery_tag'], false, true);
                    }
                    if ($ack == static::REJECT) {
                        /** 重复投递 */
                        static::$channel->basic_reject($msg->delivery_info['delivery_tag'], true);
                    }
                } else {
                    // 类或方法不存在时拒绝消息并不重新投递，因为这是代码错误，不能把消息重复投递消耗资源
                    static::$channel->basic_reject($msg->delivery_info['delivery_tag'], false);
                    call_user_func([$class, 'error'], new \RuntimeException(json_encode(['message' => "method 'handle' doesn't exists", 'data' => $params]), JSON_UNESCAPED_UNICODE));
                }

            } catch (\Exception|\RuntimeException $exception) {
                // 类或方法不存在时拒绝消息并不重新投递，这里是逻辑错误，应该由业务来修正，不能重复投递消耗资源
                static::$channel->basic_reject($msg->delivery_info['delivery_tag'], false);
                /** 消费失败，是业务的问题，这里不做处理 */
                call_user_func([$class, 'error'], new \RuntimeException(json_encode(['message' => $exception->getMessage(), 'data' => $params]), JSON_UNESCAPED_UNICODE));
            }
            /** 如果设置了本次消费个数，则剩余可消费数-1 */
            if (static::$total > 0) {
                static::$remain--;
            }
        };

        /** 设置消费者智能分配模式：就是当前消费者消费完了才接收新的消息，交换机分配的时候优先分配给空闲的消费者 */
        static::$channel->basic_qos(0, 1, false);
        /** 开始消费队里里面的消息 这里要注意一下，第二个参数添加了标签，主要是用来后面关闭通道使用，并且不会接收本消费者发送的消息*/
        static::$channel->basic_consume(static::$queueName, static::$queueName, false, false, false, false, $function);
        /** 如果有配置了回调方法，则等待接收消息。这里不建议休眠，因为设置了消息确认，会导致rabbitmq疯狂发送消息，如果取消了消息确认，休眠会导致消息丢失 */
        try {
            /** 如果设置了消费个数，则只消费指定个数后退出 */
            if (static::$total) {
                while (static::$remain && count(static::$channel->callbacks)) {
                    static::$channel->wait();
                }
            } else {
                /** 未设置指定消费个数，则常驻内存消费 */
                while (count(static::$channel->callbacks)) {
                    static::$channel->wait();
                }
            }
        } catch (\PhpAmqpLib\Exception\AMQPConnectionClosedException $exception) {
            call_user_func([get_called_class(), 'error'], new \RuntimeException(json_encode(['message' => "rabbitmq server has gone away",]), JSON_UNESCAPED_UNICODE));
            static::close();
            sleep(static::getSleepTime());
            static::make();
            static::consumeDelay();
        }
        /** 关闭队列 */
        static::close();
    }

    /**
     * 投递消息
     * @param array $msg 消息体
     * @param int $time 延迟时间 需要安装延迟插件
     * @return bool true表示投递成功 false表示投递失败
     * @throws \Exception
     */
    public static function publish(array $msg, int $time = 0)
    {
        /** 投递消息不尝试重连，因为本来就是削峰，提升响应速度 */
        static::$maxRetryConnect = 0;
        return static::sendDelay(json_encode($msg, JSON_UNESCAPED_UNICODE), $time);
    }

    /**
     * 开启消费
     * @param int $count 本次消费次数 若大于0则只消费指定条数消息退出，否则为常驻内存进程
     * @return void
     * @throws \Exception
     * @comment 本函数是阻塞的
     */
    public static function consume(int $count = 0)
    {
        static::$total = static::$remain = $count;
        static::consumeDelay();
    }
}