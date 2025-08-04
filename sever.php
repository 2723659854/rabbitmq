<?php

namespace xiaosongshu\test;

require_once __DIR__ . '/vendor/autoload.php';

/**
 * demo
 * @purpose 定义一个队列演示
 * @author yanglong
 * @time 2025年7月23日15:34:29
 * @note 当前版本客户端支持死信队列。自动维护客户端登录。
 * @warning 不要在任何地方使用exit函数，以免引起客户端掉线。
 */
class Demo extends \Xiaosongshu\Rabbitmq\Client
{
    /** 以下是rabbitmq配置 ，请填写您自己的配置 */

    /** @var string $host 服务器地址 */
    public static $host = "192.168.110.72";

    /** @var int $port 服务器端口 */
    public static $port = 5672;

    /** @var string $user 服务器登陆用户 */
    public static $user = "admin";

    /** @var string $pass 服务器登陆密码 */
    public static $pass = "123456";
    /** 指定队列名称 */
    public static $queueName = 'xiaosongshu\test\Demo';
    /** 指定交换机名称 */
    public static $exchangeName = 'xiaosongshu\test\Demo';

    /** @var bool $enableDlx 是否开启死信队列 */
    public static $enableDlx = true;

    /**
     * 业务处理
     * @param array $params
     * @return int
     * @note 你的业务代码应该写在这里，但是建议你的业务逻辑执行时间不要超过60秒，以免引起掉线风险。若你的逻辑执行时间确实超过了60秒，那么建议使用
     * 定时任务或者脚本来执行。
     */
    public static function handle(array $params): int
    {
        // todo 这里需要编写你的正常业务处理逻辑，当前仅为示例代码
        var_dump("正常队列处理",$params);
        static::log(getmypid(),"正常消费",$params);
        # 模拟业务逻辑阻塞
        sleep(10);
        /** 成功，返回ack */
        return self::ACK;
    }

    public static function log(int $pid ,string $message,array $param)
    {
        file_put_contents(__DIR__.'/log.txt',date("Y-m-d H:i:s")." 进程 [{$pid}] 消息：".$message." 参数：".json_encode($param,JSON_UNESCAPED_UNICODE)."\r\n",FILE_APPEND);
    }

    /**
     * 处理异常消息
     * @param \RuntimeException $exception
     * @return void
     */
    public static function error(\RuntimeException $exception)
    {
        //todo 这里写对于异常的处理，比如日志记录，发送短信通知、邮件通知等等，或者人工介入处理
        var_dump("发生了异常",$exception->getMessage());
    }

    /**
     * 死信队列处理逻辑
     * @param array $params
     * @return int
     * @note 此处尽量不要编写需要执行60秒以上的业务逻辑，以免引起客户端掉线，若你的业务确实超过了60秒，建议使用单独的定任务或者脚本来处理。
     * @note 此处属于兜底业务逻辑，不建议写太复杂的处理方法，本方法只是作为过期消息，异常消息的一个补充，这只是最后一根稻草。切勿编写过于复杂的逻辑业务。
     */
    public static function dlxHandle(array $params):int
    {
        //todo 这里写死信队列的处理逻辑，若不开启死信队列，则不需要写任何逻辑，直接返回ACK即可
        var_dump("死信队列处理",$params);
        static::log(getmypid(),"死信消费",$params);
        return self::ACK;
    }
}
