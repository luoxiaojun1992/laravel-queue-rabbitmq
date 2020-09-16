<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs;

use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits\QueueJobMapping;

class AggQueueConsumeJob extends Job
{
    use QueueJobMapping;

    public function handle()
    {
        $realQueue = $this->data['queue'];

        $realJobClass = $this->getJobByQueueName($realQueue, config('queue.connections.rabbitmq'));

        $data = $this->data['msg'];
        $dataArr = json_decode($data, true);
        if (!json_last_error()) {
            $data = $dataArr;
        }

        $job = new $realJobClass($data);
        $job->queue = $realQueue;
        if ($job instanceof \VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\Job) {
            $job->topic = $this->topic;
            $job->exchange = $this->exchange;
        }
        if ($job instanceof AbstractAggQueueJob) {
            $job->data = $data;
        }
        $job->setJob($this->job);

        return call_user_func([$job, 'handle']);
    }
}
