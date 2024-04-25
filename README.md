# rabbitmq queue 消息队列

## 项目地址：https://github.com/2723659854/rabbitmq
## 安装方法 install

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
#### 关闭消息消费

```php
\app\commands\Demo::close();
```

### 异常 Exception

队列使用过程中请使用 \RuntimeException和\Exception捕获异常

#### 若需要使用延迟队列，那么rabbitmq服务需要安装延迟插件，否则会报错

##### 联系作者：2723659854@qq.com ，你也可以直接提<a href="https://github.com/2723659854/rabbitmq/issues" >issues</a>
