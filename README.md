# rabbitmq queue 消息队列

## 项目地址：https://github.com/2723659854/rabbitmq

###  项目介绍
消息队列主要用于耗时业务解耦，本项目采用rabbitmq扩展，支持thinkPHP，laravel，webman，yii等常用框架，也可以单独使用。<br>
本项目经过实际生产环境测试，可以放心使用。编写此插件的原因是，有些公司项目极其老旧，而这些项目本身不支持rabbitmq队列解耦复杂业务，而市面上新版的插件
直接安装使用会各种冲突，抛出各种异常，所以作者决定自己手搓了一个。
### 安装方法 install

```shell
composer require xiaosongshu/rabbitmq
```

### 示例 demo
本实例你可以直接复制粘贴到本地测试。
#### 定义一个队列 queue

编写一个客户端server.php文件，内容如下：
```text
PS：
1，强烈建议为每一个队列指定队列名称和交换机名称，因为实际生产环境下，会存在消费者交叉嵌套的情况。比如在A消费者内给B投递消息，B消费者给C投递消息，以此类推。作者本人的项目确实存在这种逻辑，所以强烈建议指定消费者的队列名和交换机名，否则会消息投递错误，会非常混乱。
2，关于死信队列，你需要知道“参数设置后不可更改”原则，如果你需要切换开启或者关闭死信队列，你首先需要先将已进入队列的所有消息消费完毕之后，删除原来的队列，然后重建新的队列。这属于危险操作，本客户端不敢贸然删除任何队列和数据，所以需要你自己手动处理。
```
```php
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
        if (isset($params['id'])){
            if ($params['id']%3 ==0){
                /** 实际场景不可以抛出任何异常，应用捕获所有异常并处理 ，以免引起程序崩溃，虽然底层已经做了兜底 */
                throw new \Exception("模拟程序故障，抛出异常。");
            }
        }
        # 模拟业务逻辑阻塞
        sleep(4);
        /** 成功，返回ack */
        return self::ACK;
    }

    /**
     * 处理异常消息
     * @param \RuntimeException $exception
     * @return void
     */
    public static function error(\RuntimeException $exception)
    {
        //todo 这里写对于异常的处理，比如日志记录，发送短信通知、邮件通知等等，或者人工介入处理
        //var_dump("发生了异常",$exception->getMessage());
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
        //var_dump("死信队列处理",$params);
        return self::ACK;
    }
}
```

#### 投递消息 publish
编写一个publish.php文件，内容如下：
```php
<?php

namespace xiaosongshu\test;

require_once __DIR__ . '/sever.php';

for ($i = 0; $i < 100; $i++) {
    echo $i . "\r\n";
    /** 投递普通消息 */
    \xiaosongshu\test\Demo::publish(['name' => 'tom', 'time' => time(), 'id' => $i]);
    sleep(1);
}
echo "投递完成\r\n";
```
你可以在任何地方投递消息。

####   创建消费者
创建一个consume.php文件，内容如下:
```php
<?php
namespace xiaosongshu\test;

require_once __DIR__ . '/sever.php';

/** 开启消费，本函数为阻塞，后面的代码不会执行 */
\xiaosongshu\test\Demo::consume();
```
如果你的操作系统是windows环境，而且你如果开启了死信队列。那么还需要建一个文件consumeD.php用来消费死信队列的消息，内容如下：
```php
<?php

namespace xiaosongshu\test;

require_once __DIR__ . '/sever.php';

/** 开启消费，本函数为阻塞，后面的代码不会执行，仅用于windows系统调试，linux系统会自动消费死信队列的消息 */
\xiaosongshu\test\Demo::consumeD();
```
在编写完以上文件之后，就可以测试了。<br>
开启普通消息消费，在新的cmd窗口执行：
```bash
php consume.php
```
开启死信消息消费（仅限windows系统，并且开启了死信队列。linux系统不需要此命令，会自动维护死信消息的消费），在新的cmd窗口执行：
```bash
php consumeD.php
```
投递消息，在新的cmd窗口执行：
```bash
php publish.php
```
完成以上步骤后，你将会在消费者cmd窗口看到消息被投递和消费。<br>
而若需要关闭消费和投递，可以直接按键`ctrl` + `c` 停止执行。
### 在常用框架中的应用举例

你可以把消费者放到command命令行里面，使用命令行执行队列消费。举个例子(这里以yii为例子，你也可以换成laravel，webman,thinkPHP等其他框架)：<br>
####  yii中的应用举例
构建一个客户端
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
    
     /** 指定队列名称 */
    public static $queueName = 'app\commands\Demo';
    /** 指定交换机名称 */
    public static $exchangeName = 'app\commands\Demo';
    
    /** @var bool $enableDlx 是否开启死信队列 */
    public static $enableDlx = true;
    
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
    
    public static function dlxHandle(array $params):int
    {
        //todo 这里写死信队列的处理逻辑，若不开启死信队列，则不需要写任何逻辑，直接返回ACK即可
        //var_dump("死信队列处理",$params);
        return self::ACK;
    }
}

```
将消费者写入到命令行工具里面。
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
开启消费者命令 consume(你可以开启多个窗口执行此命令实现开启多个消费者的目的)，
```bash
php yii queue/index
```
如果你在linux环境下想实现无人值守模式，命令如下（你可以多次执行此命令，实现开启多个消费者的目的）：
```bash
nohup php yii queue/index >/dev/null 2>&1 &
```
投递消息方式如下，你可以在任何地方包括但不限于控制器或命令行，写入以下命令：
```php
\app\commands\Demo::publish(['name'=>'tome','age'=>15]);
```
注：如果你需要开启多个消费者，那么可以在多个窗口执行开启消费者命令即可。当然你也可以使用多进程来处理。
#### 关闭消费者
关闭消费者，在windows/linux普通环境下，你只需要在cli窗口输入`ctrl`+`c`组合键直接关闭，若你使用的是linux无人值守模式，那么需要杀死消费者进程，首先查询消费者PID
```bash
ps -ef|grep "app\commands\Demo"
```
然后使用kill命令杀死进程即可，(若开启了死信队列，需要先杀死主队列进程，然后才可以杀死死信队列进程，否则主队列会自动恢复死信队列)命令如下
```bash
kill -9 PID
```
####  thinkphp中的应用举例

以下是在thinphp框架中植入队列的示例，仅供参考。(作者在thinkphp3.2和5.1中已测试过，当前实例版本是5.1)<br>
首先创建客户端服务类RabbitmqService.php，内容如下：
```php
<?php

namespace app\service;

require_once dirname(__DIR__,3) . '/vendor/autoload.php';

/**
 * @purpose rabbitmq队列服务，用于写日志，防止多进程日志相互覆盖
 * @author yanglong
 * @time 2025年6月20日12:49:24
 */
class RabbitmqService extends \Xiaosongshu\Rabbitmq\Client
{

    /** 以下是rabbitmq配置 ，请填写您自己的配置 */
    /** @var string $host 服务器地址 */
    public static $host = "127.0.0.1";

    /** @var int $port 服务器端口 */
    public static $port = 5672;

    /** @var string $user 服务器登陆用户 */

    public static $user = "admin";

    /** @var string $pass 服务器登陆密码 */
   
    public static $pass = "123456";

    /** 指定交换机 */
    public static $exchangeName = "app\service\RabbitmqService";

    /** 指定队列 */
    public static $queueName = "app\service\RabbitmqService";

    /** @var bool $enableDlx 是否开启死信队列 */
    public static $enableDlx = true;
    
    /**
     * 业务处理
     * @param array $params
     * @return int
     */
    public static function handle(array $params): int
    {
        if (!empty($params['file']) && !empty($params['content'])){
            /** 写入日志 */
            Token::writeLog($params['file'], $params['content']);
        }
        return self::ACK;
    }

    /**
     * 处理错误逻辑
     * @param \RuntimeException $exception
     * @return void
     */
    public static function error(\RuntimeException $exception)
    {
        /** 此处只是示例，不做具体逻辑处理 */
    }
    
    public static function dlxHandle(array $params):int
    {
        /** 此处只是示例，不做具体逻辑处理 */
        return self::ACK;
    }
}
```
启动消费者，我们使用thinkphp的命令行工具实现，内容如下：
```php
<?php

namespace app\command;

use app\service\RabbitmqService;
use app\service\Token;
use think\console\Command;
use think\console\Input;
use think\console\Output;

/**
 * @purpose 异步写日志服务
 * @author yanglong
 * @time 2025年6月20日15:19:59
 * @command nohup php think check:rabbitmq >/dev/null 2>&1 &
 */
class CheckRabbitmq extends Command
{
    protected function configure()
    {
        // 指令配置
        $this->setName('check:rabbitmq');
        // 设置参数
        
    }

    protected function execute(Input $input, Output $output)
    {
        /** 开启消费者 */
        RabbitmqService::consume();
    }
}
```
开启消费命令如下(你可以开启多个窗口执行此命令实现开启多个消费者的目的)：
```bash
php think check:rabbitmq
```
如果需要在linux上已无人值守模式运行，命令如下(你可以多次执行此命令，实现开启多个消费者的目的)：
```bash 
nohup php think check:rabbitmq >/dev/null 2>&1 &
```
如果你的是windows系统，需要测试死信队列，那么还需要创建一个死信队列的消费者，内容如下：
```php
<?php

namespace app\command;

use app\service\RabbitmqService;
use app\service\Token;
use think\console\Command;
use think\console\Input;
use think\console\Output;

/**
 * @purpose 异步写日志服务
 * @author yanglong
 * @time 2025年6月20日15:19:59
 * @command nohup php think check:rabbitmqD >/dev/null 2>&1 &
 */
class CheckRabbitmq extends Command
{
    protected function configure()
    {
        // 指令配置
        $this->setName('check:rabbitmqD');
        // 设置参数
        
    }

    protected function execute(Input $input, Output $output)
    {
        /** 开启死信消费者 */
        RabbitmqService::consumeD();
    }
}

```
开启死信消费者命令是(你可以开启多个窗口执行此命令实现开启多个消费者的目的)：
```bash
php think check:rabbitmqD
```
若需要在linux环境下无人值守模式运行，命令如下(你可以多次执行此命令，实现开启多个消费者的目的)：
```bash
nohup php think check:rabbitmqD >/dev/null 2>&1 &
```
投递消息：你可以在任何地方投递消息，包括但不限于控制器、命令行，只需要在代码中加入以下方法即可投递消息：
```php
RabbitmqService::publish(['file'=>$file, 'content'=>$content]);
```
#### 关闭消费者
关闭消费者，在windows/linux普通环境下，你只需要在cli窗口输入`ctrl`+`c`组合键直接关闭，若你使用的是linux无人值守模式，那么需要杀死消费者进程，首先查询消费者PID
```bash
ps -ef|grep "app\service\RabbitmqService"
```
然后使用kill命令杀死进程即可，(若开启了死信队列，需要先杀死主队列进程，然后才可以杀死死信队列进程，否则主队列会自动恢复死信队列)命令如下
```bash
kill -9 PID
```
作者尽量将本插件的使用方法贴出了示例，是希望各位用户明白使用方法，但是天下框架何其多，也不可能给所有的框架写示例。当明白了使用方法的时候，任何框架都可以嫁接进去使用。
当然了，希望你不要照搬我的示例，因为我的代码仅仅是示例，可能不完全符合你的业务需求。
### 异常 Exception

队列使用过程中请使用 \RuntimeException和\Exception捕获异常。
###  特别提示

如果使用thinkphp3.2作为项目框架的时候，作者在实际项目中遇到内存泄漏的问题，经过排查是thinkphp3.2的数据库模型的M方法存在问题，当队列高频次操作数据库
的时候，队列连续运行72小时左右，内存占用就会暴涨到1G左右，所以建议不要在队列中使用模型。但是实际业务通常是需要操作数据库的，那么有两种解决办法。<br>
第一种：使用原生的mysql，实例如下所示（此处仅为示例代码，请根据实际情况自行编写代码）:
```php
 /**
  * 业务逻辑 
  */
 public static function handle(array $params): int
 {
    try{
        $db = new \mysqli("127.0.0.1", "demo", "root", "root", 3306);
        $res = $db->query("select * from users");
        while($row = $res->fetch_assoc()){
            var_dump($row);
        }
        $res->free();
        $db->close();
    }catch (\Exception $exception){

    }
    return self::ACK;
}
```
第二种：使用thinkphp的cli模式<br>
我们都知道，thinkphp3.2也是支持cli模式的，比如访问路由`/home/index/index`的时候，除了使用`http://your.domain.com/home/index/index` 的方式访问，
还可以使用cli命令行访问，在cmd窗口项目根目录执行`php index.php /home/index/index`即可访问。那么我们可以将真实的业务写在 `/home/index/index`下。
那么我们的队列的业务逻辑应该如下：
```php
 /**
  * 业务逻辑 
  */
 public static function handle(array $params): int
 {
    try{
        $id = $params['id'];
        # 你可以传入任意参数，但是需要你自己在/home/index/index里手动解析这些参数
        $cmd = "timeout -s 3 10 php index.php /home/index/index --id={$id}";
    }catch (\Exception $exception){

    }
    return self::ACK;
}
```
实际的业务方法`/home/index/index`如下：
```php
 /**
  * 业务类
  * @return void
  * @note 业务本不需要如此复杂，被逼无奈，出此下策。
  */
  public function index()
  {
    # 此处将会打印出通过上面的命令传入的参数，可能需要你自己来解析这些参数，这些都是很简单的，就不写示例了。
    $argv = $_SERVER['argv'];
    var_dump($argv);
  }
```
你也可以单独编写一个入口文件`cli.php`代替原来的`index.php`。将fpm和cli拆分开来，然后在`cli.php`中编写解析入参的函数。是不是感觉搞得好复杂，是就对了。如果你是使用的
thinkphp5以上的版本或者laravel5以上等等比较新的框架作为项目底层架构，他们已经有成熟的队列插件，你就不需要使用本插件查看本文档了，本插件只是给予一种新的解决方案而已。

#### 若需要使用延迟队列，那么rabbitmq服务需要安装延迟插件，否则会报错

##### 联系作者：2723659854@qq.com ，你也可以直接提<a href="https://github.com/2723659854/rabbitmq/issues" >issues</a>

### 一键搭建rabbitmq服务
我们可以使用docker容器一键搭建rabbitmq服务，其中<br>
`-p 5671:5671 -p 5672:5672 -p 15671:15671 -p 15672:15672 -p 61613:61613`是端口映射(-p 宿主机端口:容器内服务端口)，<br>
`/d/rabbitmq/:/var/log/rabbitmq/`是目录挂载（目录映射 宿主机目录：容器内目录，请根据实际工作环境修改），<br>
`-e RABBITMQ_DEFAULT_USER=admin`是rabbitmq的登录用户名，你可以根据你的需求修改<br>
`-e RABBITMQ_DEFAULT_PASS=123456`是你的rabbitmq的登录密码，你可以根据需求修改。
```bash
docker run -d --restart always --name faster-rabbitmq -p 5671:5671 -p 5672:5672 -p 15671:15671 -p 15672:15672 -p 61613:61613 -v /d/rabbitmq/:/var/log/rabbitmq/ -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=123456 -e RABBITMQ_DEFAULT_VHOST=/ rabbitmq:3.8-management-alpine
```

