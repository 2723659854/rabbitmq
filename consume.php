<?php
namespace xiaosongshu\test;

require_once __DIR__ . '/sever.php';

/** 开启消费，本函数为阻塞，后面的代码不会执行 */
\xiaosongshu\test\Demo::consume();
/** 开启消费，消费5个消息后自动退出，用于测试 */
// \xiaosongshu\test\Demo::consume(5);

