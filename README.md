# rabbitmq 延迟队列
#### 支持laravel,thinkphp,webman框架，也可单独使用。
## 安装方法

```shell
composer require xiaosongshu/rabbitmq
```
### 配置

webman,laravel,thinkphp 等常用框架，请在config目录下创建rabbitmq.php配置文件，配置参数内容如下：
```php 
/** rabbitmq 基本配置 */
return [
    'host'       => '127.0.0.1',
    'port'       => '5672',
    'username'   => 'guest',
    'password'   => 'guest',
];
```
你也可以手动传入配置参数，如下所示：

```php 
$rabbit = new Client(
    'exchangeName',
    'queueName',
    'direct',
    [
        'host' => '127.0.0.1',
        'port' => '5672',
        'username' => 'guest',
        'password' => 'guest',
    ]
);
```

### 投递消息 send.php

```php
use Xiaosongshu\Rabbitmq\Client;
/** 定义消息 */
$msg = ['status'=>200,'msg'=>date('Y-m-d H:i:s')];
/** 1，投递普通消息 */
(new Client())->send($msg);
/** 2，投递延迟消息，延迟2秒 */
(new Client())->send($msg,$time=2);
```

### 消费消息 consume.php
```php 
use Xiaosongshu\Rabbitmq\Client;
/** 方式一 通过定义callback方法处理消息 */
$rabbit = new Client();
$rabbit->callback=function($params){ var_dump($params); };
/** 开始消费数据 */
$rabbit->consume();
/** 方式二 继承Client类，重写handle方法 */
class Test extends Client
{

    /**
     * 自定义的handle方法处理消息
     * @param array $param
     * @return mixed
     */
    public function handle(array $param): mixed
    {
        var_dump($param);
        //todo 自行实现逻辑
    }
}
/** 实例化消费者类 */
$client = new Test();
/** 开始消费数据 */
$client->consume();

```
### 开启消费者

```bash 
php your_project_path/consume.php 
```
#### 或者放入到其它常驻内存的进程中，比如webman，单开一个常驻内存放消费者


### 修改队列名称，交换机名称，在实例化的时候传入即可
```php 
$rabbit = new Client('exchangeName','queueName','direct');
```
### 异常
队列使用过程中请使用 \RuntimeException和\Exception捕获异常
#### 若需要使用延迟队列，那么rabbitmq服务需要安装延迟插件，否则会报错

##### 联系作者：2723659854@qq.com
