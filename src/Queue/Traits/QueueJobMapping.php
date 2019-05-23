<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits;

use VladimirYuldashev\LaravelQueueRabbitMQ\Utils\ArrayUtil;

trait QueueJobMapping
{
    /**
     * @param $queueName
     * @param $config
     * @param null $routingKey
     * @return mixed
     */
    protected function getJobByQueueName($queueName, $config, $routingKey = null)
    {
        $queueJobMapping = $config['options']['consumer']['queueJobMapping'];

        $jobClass = null;

        if (isset($routingKey)) { //match with queue & routingKey
            if (!empty($config['options']['exchange']['routing_key_prefix'])) {
                $routingKey = str_replace(
                    $config['options']['exchange']['routing_key_prefix'] . '.', '',
                    $routingKey
                );
            }

            if (isset($queueJobMapping[$queueName][$routingKey])) { //full match with queue & routingKey, for performance
                $jobClass = $queueJobMapping[$queueName][$routingKey];
            } elseif (isset($queueJobMapping[$queueName])) { //match with queue & routingKey
                if (is_array($queueJobMapping[$queueName])) { //regex match with routingKey
                    $jobClass = ArrayUtil::searchByRegexKey($routingKey, $queueJobMapping[$queueName]);
                } else { //full match with queue only
                    $jobClass = $queueJobMapping[$queueName];
                }
            }

            if (empty($jobClass)) {
                $this->triggerInvalidJobException($queueName . '|' . $routingKey);
            }
        } else { //match with queue only
            if (!empty($queueJobMapping[$queueName])) { //full match with queue only
                $jobClass = $queueJobMapping[$queueName];
            } else {
                $this->triggerInvalidJobException($queueName);
            }
        }

        return $jobClass;
    }

    private function triggerInvalidJobException($msg)
    {
        throw new \RuntimeException(
            'Invalid JOB QUEUE MAPPING : ' . $msg,
            1
        );
    }
}
