<?php

namespace xiaosongshu\test;


require_once __DIR__ . '/vendor/autoload.php';

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

        /** 成功，返回ack */
        return self::ACK;
        /** 失败，返回NACK*/
        //return self::NACK;
    }

    /**
     * 处理异常消息
     * @param \RuntimeException $exception
     * @return void
     */
    public static function error(\RuntimeException $exception)
    {
        var_dump("捕获到了异常",$exception->getMessage());
    }
}

/** 投递普通消息 */
\xiaosongshu\test\Demo::publish(['name' => 'tom']);
\xiaosongshu\test\Demo::publish(['name' => 'jim']);
\xiaosongshu\test\Demo::publish(['name' => 'jack']);
//for($i=0;$i<100;$i++){
//    \xiaosongshu\test\Demo::publish(['name' => 'tom','num'=>$i]);
//}
/** 开启消费，本函数为阻塞，后面的代码不会执行 */
\xiaosongshu\test\Demo::consume();
/** 关闭消费者 */
\xiaosongshu\test\Demo::close();