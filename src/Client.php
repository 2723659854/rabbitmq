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
abstract class   Client implements RabbiMQInterface
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
    private static $channel;
    /** @var AMQPStreamConnection rabbitmq连接 */
    private static $connection;

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
    private static  $instance = null;

    /**
     * 初始化相关配置，建立链接
     * @return void
     */
    private static function make()
    {
        /** 初始化订阅方式 */
        if (!self::$type) {
            self::$type = self::EXCHANGETYPE_DIRECT;
        }
        $className = get_called_class();
        /** 初始化交换机 */
        if (!self::$exchangeName) {
            self::$exchangeName = $className;
        }
        /** 初始化队列 */
        if (!self::$queueName) {
            self::$queueName = $className;
        }
        /**  创建一个rabbitmq连接*/
        try {
            self::$connection = new AMQPStreamConnection(self::$host, self::$port, self::$user, self::$pass);
        } catch (\Exception|\RuntimeException|AMQPRuntimeException|AMQPConnectionBlockedException $exception) {
            throw new \RuntimeException($exception->getMessage());
        }

        /**  创建一个通道*/
        self::$channel = self::$connection->channel();
        /** 声明交换机 */
        self::$channel->exchange_declare(self::$exchangeName, self::$type ?: self::EXCHANGETYPE_DIRECT, false, true, false, false, false, new AMQPTable(["x-delayed-type" => self::EXCHANGETYPE_DIRECT]));
        /** 声明队列  */
        self::$channel->queue_declare(self::$queueName, false, true, false, false);
        /** 将队列绑定到交换机 同时设置路由，*/
        self::$channel->queue_bind(self::$queueName, self::$exchangeName, self::$queueName);
        /** 保存链接 */
        self::$instance = self::$connection;
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
     * 发送延迟消息
     * @param string $msg 消息内容
     * @param int $delay 延迟时间
     * @return void
     * @throws \Exception
     */
    private static function sendDelay(string $msg, int $delay = 0)
    {
        /** 检查链接 */
        if (!self::$connection) {
            self::make();
        }
        self::$timeOut = $delay;
        /** @var AMQPMessage $_msg 创建rabbitmq的延迟消息 */
        $_msg = self::createMessageDelay($msg, self::$timeOut);
        /** 发布消息 语法：消息体，交换机，路由（这里作者简化了用的队列名称代理路由名称）*/
        self::$channel->basic_publish($_msg, self::$exchangeName, self::$queueName);
    }

    /**
     * 关闭服务
     * @return void
     * @throws \Exception
     */
    public static function close()
    {
        if (self::$instance){
            /** 发布完成后关闭通道 */
            self::$channel->close();
            /** 发布完成后关闭连接 */
            self::$connection->close();
        }
    }

    /**
     * 消费延迟队列
     * @return void
     * @throws \Exception
     */
    private static function consumeDelay()
    {
        /** 检查链接 */
        if (!self::$instance) {
            self::make();
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
                    //$ack = static::handle($params);
                    if ($ack == self::ACK) {
                        /** 确认接收到消息 */
                        self::$channel->basic_ack($msg->delivery_info['delivery_tag'], false);
                    }
                    if ($ack == self::NACK) {
                        /** 重复投递 */
                        self::$channel->basic_nack($msg->delivery_info['delivery_tag'], false, true);
                    }
                    if ($ack == self::REJECT) {
                        /** 重复投递 */
                        self::$channel->basic_reject($msg->delivery_info['delivery_tag'], true);
                    }
                } else {
                    throw new \Exception("No 'handle' method found");
                }

            } catch (\Exception|\RuntimeException $exception) {
                /** 消费失败，是业务的问题，这里不做处理 */
                throw new \RuntimeException($exception->getMessage());
            }
        };

        /** 设置消费者智能分配模式：就是当前消费者消费完了才接收新的消息，交换机分配的时候优先分配给空闲的消费者 */
        self::$channel->basic_qos(0, 1, false);
        /** 开始消费队里里面的消息 这里要注意一下，第二个参数添加了标签，主要是用来后面关闭通道使用，并且不会接收本消费者发送的消息*/
        //TODO 这里没有处理死信队列
        self::$channel->basic_consume(self::$queueName, self::$queueName, false, false, false, false, $function);
        /** 如果有配置了回调方法，则等待接收消息。这里不建议休眠，因为设置了消息确认，会导致rabbitmq疯狂发送消息，如果取消了消息确认，休眠会导致消息丢失 */
        while (count(self::$channel->callbacks)) {
            self::$channel->wait();
        }
        self::close();
    }

    /**
     * 投递消息
     * @param array $msg 消息内容
     * @param int $time 延迟时间
     * @return void
     * @throws \Exception
     */
    public static function publish(array $msg, int $time = 0)
    {
        self::sendDelay(json_encode($msg), $time);
    }

    /**
     * 开启消费
     * @return void
     * @throws \Exception
     * @comment 本函数是阻塞的
     */
    public static function consume()
    {
        self::consumeDelay();
    }
}