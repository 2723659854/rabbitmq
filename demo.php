<?php
namespace root\yanglong;

require_once __DIR__.'/vendor/autoload.php';

/**
 * demo
 * @purpose 定义一个队列演示
 */
class Demo extends \Xiaosongshu\Rabbitmq\Client
{

    /** 以下是rabbitmq配置 ，请填写您自己的配置 */
    /** @var string $host 服务器地址 */
    public static $host = "127.0.0.1";

    /** @var int $port 服务器端口 */
    public static $port = 5672;

    /** @var string $user 服务器登陆用户 */
    public static $user = "guest";

    /** @var string $pass 服务器登陆密码 */
    public static $pass = "guest";

    /**
     * 业务处理
     * @param array $params
     * @return int
     */
    public static function handle(array $params): int
    {
        //TODO 这里写你的业务逻辑
        // ...
        var_dump($params);
        return self::ACK;
        //return self::NACK;
    }
}

/** 投递普通消息 */
\root\yanglong\Demo::publish(['name' => 'tom']);
\root\yanglong\Demo::publish(['name' => 'jim']);
\root\yanglong\Demo::publish(['name' => 'jack']);
/** 开启消费 */
\root\yanglong\Demo::consume();