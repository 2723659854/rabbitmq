<?php

namespace Xiaosongshu\Rabbitmq;

interface RabbiMQInterface
{
    public static function handle(array $params):int;

    public static function error(\RuntimeException $exception):void;

    public static function dlxHandle(array $params):int;
}