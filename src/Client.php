<?php

namespace Xiaosongshu\Rabbitmq;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * @purpose rabbitmq投递和消费
 * @example 投递普通消息  (new Client())->send(['status'=>200,'msg'=>date('Y-m-d H:i:s')]);
 * @example 消费消息 方式1：
 * $rabbit = new Client();
 * $rabbit->callback=function($params){ var_dump($params); };
 * $rabbit->consume();
 * @example 消费消息 方式2：继承Client ,然后重写handle方法。
 * @note 如果需要投递延迟消息，需要安装延迟插件，投递消息：(new Client())->send(array $msg,int $time=2); 延迟时间单位秒
 */
class Client
{
    /** @var string 服务器地址 */
    private $host = "127.0.0.1";
    /** @var int 服务器端口 */
    private $port = 5672;
    /** @var string 服务器登陆用户 */
    private $user = "guest";
    /** @var string 服务器登陆密码 */
    private $pass = "guest";
    /** @var \PhpAmqpLib\Channel\AbstractChannel|\PhpAmqpLib\Channel\AMQPChannel 渠道通道 */
    private $channel;
    /** @var AMQPStreamConnection rabbitmq连接 */
    private $connection;


    /** @var int 过期时间 */
    private $timeOut = 0;
    /** @var string 交换机名称 */
    private $exchangeDelayed = "delayed";
    /** @var string 队列名称 */
    private $queueName = "delayedQueue";

    /** @var string 交换机类型 转发给所有绑定到本交换机的通道，不匹配路由 */
    const EXCHANGETYPE_FANOUT = "fanout";
    /** @var string 交换机类型 只转发给绑定到本交换机，并且路由完全匹配的通道 */
    const EXCHANGETYPE_DIRECT = "direct";
    /** @var string 交换机类型  延迟信息 */
    const EXCHANGETYPE_DELAYED = "x-delayed-message";

    /** @var int 最大尝试次数 */
    public $_max_attempt_num = 3;

    /** 回调函数，用来处理业务逻辑 */
    public $callback = null;


    /**
     * 初始化rabbitmq连接
     * @param string $exchangeName 交换机名称
     * @param string $queueName 队列名称
     * @param string $type 交换机类型，如果需要延迟消费，则需要安装延迟插件
     * @param array $hostConfig rabbitmq配置
     */
    public function __construct(string $exchangeName = '', string $queueName = '', string $type = '', array $hostConfig = [])
    {
        /** 兼容没有config函数的框架，支持手动传入rabbitmq服务器配置 */
        try {
            $config = config('rabbitmq');
        } catch (\Exception $exception) {
            if (!empty($hostConfig)) {
                $config = $hostConfig;
            } else {
                throw new \RuntimeException($exception->getMessage());
            }
        }

        $this->host = $config['host'];
        $this->port = $config['port'];
        $this->user = $config['username'];
        $this->pass = $config['password'];
        /** 设置交换机名称 */
        if ($exchangeName) {
            $this->exchangeDelayed = $exchangeName;
        }
        /** 设置队列 */
        if ($queueName) {
            $this->queueName = $queueName;
        }
        /**  创建一个rabbitmq连接*/
        try {
            $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->pass);
        } catch (\Exception|\RuntimeException|AMQPRuntimeException|AMQPConnectionBlockedException $exception) {
            throw new \RuntimeException($exception->getMessage());
        }

        /**  创建一个通道*/
        $this->channel = $this->connection->channel();
        /** 声明交换机 */
        $this->channel->exchange_declare($this->exchangeDelayed, $type ?: self::EXCHANGETYPE_DIRECT, false, true, false, false, false, new AMQPTable(["x-delayed-type" => self::EXCHANGETYPE_DIRECT]));
        /** 声明队列  */
        $this->channel->queue_declare($this->queueName, false, true, false, false);
        /** 将队列绑定到交换机 同时设置路由，*/
        $this->channel->queue_bind($this->queueName, $this->exchangeDelayed, $this->queueName);
    }

    /**
     * 创建延迟信息
     * @param string $msg 消息内容
     * @param int $time 延迟时间 必须安装延迟插件，否则不能使用
     * @return AMQPMessage 包装时候的消息
     */
    private function createMessageDelay(string $msg, int $time = 0): object
    {
        /** @var  $delayConfig [] 初始化消息配置 */
        $delayConfig = [
            /** 传递模式   消息持久化 ，这一个配置是消费确认发送ack的根本原因*/
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];
        if ($time) {
            /** 消息表头 设置延迟时间  延迟可以精确到毫秒 */
            $delayConfig['application_headers'] = new AMQPTable(['x-delay' => $time * 1000]);
        }
        /** @var  $msg AMQPMessage 生成消息对象 */
        return new AMQPMessage($msg, $delayConfig);
    }

    /**
     * 发送延迟消息
     * @param string $msg 消息内容
     * @param int $delay 延迟时间
     * @return void
     * @throws \Exception
     */
    private function sendDelay(string $msg, int $delay = 0)
    {
        $this->timeOut = $delay;
        /** @var AMQPMessage $_msg 创建rabbitmq的延迟消息 */
        $_msg = $this->createMessageDelay($msg, $this->timeOut);
        /** 发布消息 语法：消息体，交换机，路由（这里作者简化了用的队列名称代理路由名称）*/
        $this->channel->basic_publish($_msg, $this->exchangeDelayed, $this->queueName);
    }

    /**
     * 关闭服务
     * @return void
     * @throws \Exception
     */
    private function close()
    {
        /** 发布完成后关闭通道 */
        $this->channel->close();
        /** 发布完成后关闭连接 */
        $this->connection->close();
    }

    /**
     * 消费延迟队列
     * @return void
     * @throws \Exception
     */
    private function consumeDelay()
    {
        /**
         * 创建一个回调函数，用来处理接收到的消息
         * @param $msg
         * @return void
         */
        $callback = function ($msg) {
            $params = json_decode($msg->body, true);
            /** 这里手动实现了队列的执行次数统计，因为是重试队列，所以不能让队列反反复复的一直重试 */
            if (!isset($params['_max_attempt_num'])) {
                $params['_max_attempt_num'] = 1;
            } else {
                $params['_max_attempt_num']++;
            }
            try {
                /** 调用用户的逻辑 */
                if (($this->callback != null) && is_callable($this->callback)) {
                    call_user_func($this->callback, $params);
                } else {
                    $this->handle($params);
                }
                /** 确认接收到消息 */
                $this->channel->basic_ack($msg->delivery_info['delivery_tag'], false);
            } catch (\Exception|\RuntimeException $exception) {
                /** 如果当前任务重试次数小于最大尝试次数，那么就继续重试， */
                if ($params['_max_attempt_num'] <= $this->_max_attempt_num) {
                    $this->sendDelay(json_encode($params), $this->timeOut);
                } else {
                    throw new \RuntimeException($exception->getMessage());
                }
                /** 确认接收到消息 */
                $this->channel->basic_ack($msg->delivery_info['delivery_tag'], false);
            }
        };
        /** 设置消费者智能分配模式：就是当前消费者消费完了才接收新的消息，交换机分配的时候优先分配给空闲的消费者 */
        $this->channel->basic_qos(null, 1, null);
        /** 开始消费队里里面的消息 这里要注意一下，第二个参数添加了标签，主要是用来后面关闭通道使用，并且不会接收本消费者发送的消息*/
        $this->channel->basic_consume($this->queueName, 'lantai', false, false, false, false, $callback);

        /** 如果有配置了回调方法，则等待接收消息。这里不建议休眠，因为设置了消息确认，会导致rabbitmq疯狂发送消息，如果取消了消息确认，休眠会导致消息丢失 */
        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }
        $this->close();
    }

    /**
     * 发送消息
     * @param array $msg
     * @param int $time
     * @return void
     * @throws \Exception
     */
    public function send(array $msg, int $time = 0)
    {
        $this->sendDelay(json_encode($msg), $time);
        $this->close();
    }

    /**
     * 消费队列
     * @return void
     * @throws \Exception
     */
    public function consume()
    {
        $this->consumeDelay();
    }

    /**
     * 业务逻辑
     * @param array $param
     * @return mixed
     * @note 如果需要使用此方法处理业务逻辑，需要用户继承本客户端类，然后自己实现handle里面的业务逻辑
     */
    public function handle(array $param): mixed
    {

    }
}