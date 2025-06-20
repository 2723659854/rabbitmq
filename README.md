# rabbitmq queue 消息队列

## 项目地址：https://github.com/2723659854/rabbitmq

###  项目介绍
消息队列主要用于业务解耦，本项目采用rabbitmq扩展，支持thinkPHP，laravel，webman，yii等常用框架，也可以单独使用。
### 安装方法 install

```shell
composer require xiaosongshu/rabbitmq
```

### 示例 demo

#### 定义一个队列 queue

```php
<?php
namespace app\commands;

require_once __DIR__.'/vendor/autoload.php';

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

```

#### 投递消息 publish
```php 
\app\commands\Demo::publish(['name'=>'tome','age'=>15]);
```
你可以在任何地方投递消息。

####   开启消费
```php
\app\commands\Demo::consume();
```
你可以把消费者放到command命令行里面，使用命令行执行队列消费。举个例子(这里以yii为例子，你也可以换成laravel，webman,thinkPHP等其他框架)：
```php 
<?php

namespace app\commands;

use yii\console\Controller;

/**
 * @purpose 开启队列消费
 * @note 我只是一个例子
 */
class QueueController extends Controller
{

    /**
     * @api php yii queue/index
     * @return void
     * @throws \Exception
     * @comment 开启消费者
     */
    public function actionIndex()
    {
        Demo::consume();
    }
}
```
开启消费者命令 consume
```bash
php yii queue/index
```
注：如果你需要开启多个消费者，那么可以在多个窗口执行开启消费者命令即可。当然你也可以使用多进程来处理。
#### 关闭消费者

```php
\app\commands\Demo::close();
```

### 异常 Exception

队列使用过程中请使用 \RuntimeException和\Exception捕获异常

#### 若需要使用延迟队列，那么rabbitmq服务需要安装延迟插件，否则会报错

### 测试
本项目根目录有一个demo.php的测试文件，可以复制到你的项目根目录，在命令行窗口直接在命令行执行以下命令即可。
```php
php demo.php
```
测试文件代码如下：
```php
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
}

/** 投递普通消息 */
\xiaosongshu\test\Demo::publish(['name' => 'tom']);
\xiaosongshu\test\Demo::publish(['name' => 'jim']);
\xiaosongshu\test\Demo::publish(['name' => 'jack']);
/** 方法1：开启消费，本函数为阻塞，后面的代码不会执行 */
\xiaosongshu\test\Demo::consume();
/** 方法2：开启消费，累计消费10条数据后退出 */
\xiaosongshu\test\Demo::consume(10);
/** 关闭消费者 */
\xiaosongshu\test\Demo::close();
```
##### 联系作者：2723659854@qq.com ，你也可以直接提<a href="https://github.com/2723659854/rabbitmq/issues" >issues</a>
