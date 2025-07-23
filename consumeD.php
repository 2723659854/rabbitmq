<?php

namespace xiaosongshu\test;

require_once __DIR__ . '/sever.php';

/** 开启消费，本函数为阻塞，后面的代码不会执行，仅用于windows系统调试，linux系统会自动消费死信队列的消息 */
\xiaosongshu\test\Demo::consumeD();