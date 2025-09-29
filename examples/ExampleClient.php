<?php

namespace Xiaosongshu\Rabbitmq\Examples;

use Xiaosongshu\Rabbitmq\Client;
use RuntimeException;

/**
 * 示例客户端（仅用于IDE类型推断和用户参考，请勿在生产环境使用）
 * @author yanglong
 * @time 2025年9月29日15:45:01
 * @note 示例代码
 */
class ExampleClient extends Client
{

    // 实例配置
    /** @var string $host 服务器地址 */
    public static $host = "127.0.0.1";

    /** @var int $port 服务器端口 */
    public static $port = 5672;

    /** @var string $user 服务器登陆用户 */
    public static $user = "guest";

    /** @var string $pass 服务器登陆密码 */
    public static $pass = "guest";

    /**
     * 示例业务处理
     */
    public static function handle(array $params): int
    {
        // 示例逻辑：简单确认
        return self::ACK;
    }

    /**
     * 示例异常处理
     */
    public static function error(RuntimeException $exception): void
    {
        // 示例逻辑：输出到控制台
        echo "[Example Error] " . $exception->getMessage() . PHP_EOL;
    }

    /**
     * 示例死信处理
     */
    public static function dlxHandle(array $params): int
    {
        // 示例逻辑：简单确认
        return self::ACK;
    }
}
?>
