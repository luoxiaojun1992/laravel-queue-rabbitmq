<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

class AliyunCredentialsProvider
{
    public static function getUser($resourceOwnerId, $accessKey)
    {
        $t = '0:' . $resourceOwnerId . ':' . $accessKey;
        return base64_encode($t);
    }

    public static function getPassword($accessSecret)
    {
        $ts = (int)(microtime(true) * 1000);
        $value = utf8_encode($accessSecret);
        $key = utf8_encode((string)$ts);
        $sig = strtoupper(hash_hmac('sha1', $value, $key, FALSE));
        return base64_encode(utf8_encode($sig . ':' . $ts));
    }
}
