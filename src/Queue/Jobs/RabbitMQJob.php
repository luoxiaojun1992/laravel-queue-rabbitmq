<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs;

use DateTimeInterface;
use Exception;
use Illuminate\Contracts\Bus\Dispatcher;
use Illuminate\Database\Eloquent\ModelNotFoundException;
use Interop\Amqp\AmqpMessage;
use Illuminate\Queue\Jobs\Job;
use Interop\Amqp\AmqpConsumer;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits\JobHandler;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits\QueueJobMapping;

class RabbitMQJob extends Job implements JobContract
{
    use JobHandler;
    use QueueJobMapping;

    /**
     * Same as RabbitMQQueue, used for attempt counts.
     */
    public const ATTEMPT_COUNT_HEADERS_KEY = 'attempts_count';

    protected $connection;
    protected $consumer;
    protected $message;
    protected $config;
    protected $job;

    public function __construct(
        Container $container,
        RabbitMQQueue $connection,
        AmqpConsumer $consumer,
        AmqpMessage $message,
        $config
    ) {
        $this->container = $container;
        $this->connection = $connection;
        $this->consumer = $consumer;
        $this->message = $message;
        $this->queue = $consumer->getQueue()->getQueueName();
        $this->connectionName = $connection->getConnectionName();
        $this->config = $config;

        $jobClass = $this->getJobClass();
        $this->job = new $jobClass($this->getData());
        $this->job->queue = $this->getQueueNameWithoutPrefix();
        if ($this->job instanceof \VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\Job) {
            $this->job->topic = $this->message->getRoutingKey();
            $this->job->exchange = $this->message->getExchangeName();
        }
    }

    protected function getQueueNameWithoutPrefix()
    {
        $queueName = $this->queue;
        $queueNamePrefix = $this->config['queue_name_prefix'] ?? null;
        if ($queueNamePrefix) {
            $queueName = substr($queueName, strlen($queueNamePrefix) + 1);
        }

        return $queueName;
    }

    /**
     * @return mixed|string
     */
    protected function getData()
    {
        $data = $this->getRawBody();
        $dataArr = json_decode($data, true);
        if (!json_last_error()) {
            $data = $dataArr;
        }

        return $data;
    }

    /**
     * Get the decoded body of the job.
     *
     * @return array
     */
    public function payload()
    {
        return [
            'displayName' => $this->getDisplayName($this->job),
            'job' => 'Illuminate\Queue\CallQueuedHandler@call',
            'maxTries' => $this->maxTries(),
            'timeout' => $this->timeout(),
            'timeoutAt' => $this->getJobExpiration($this->job),
            'data' => [
                'commandName' => get_class($this->job),
                'command' => serialize(clone $this->job),
            ],
        ];
    }

    /**
     * Get the number of times to attempt a job.
     *
     * @return int|null
     */
    public function maxTries()
    {
        return $this->job->tries ?? null;
    }

    /**
     * Get the number of seconds to delay a failed job before retrying it.
     *
     * @return int|null
     */
    public function delaySeconds()
    {
        $delayTo = $this->message->getProperty('delay_to');
        if (!is_null($delayTo)) {
            return intval($delayTo) - time();
        }

        return null;
    }

    /**
     * Get the number of seconds the job can run.
     *
     * @return int|null
     */
    public function timeout()
    {
        return $this->job->timeout ?? null;
    }

    /**
     * Get the expiration timestamp for an object-based queue handler.
     *
     * @param  mixed  $job
     * @return mixed
     */
    protected function getJobExpiration($job)
    {
        if (! method_exists($job, 'retryUntil') && ! isset($job->timeoutAt)) {
            return;
        }

        $expiration = $job->timeoutAt ?? $job->retryUntil();

        return $expiration instanceof DateTimeInterface
            ? $expiration->getTimestamp() : $expiration;
    }

    /**
     * Get the display name for the given job.
     *
     * @param  mixed  $job
     * @return string
     */
    protected function getDisplayName($job)
    {
        return method_exists($job, 'displayName')
            ? $job->displayName() : get_class($job);
    }

    /**
     * Get the timestamp indicating when the job should timeout.
     *
     * @return int|null
     */
    public function timeoutAt()
    {
        return $this->getJobExpiration($this->job);
    }

    /**
     * Get the name of the queued job class.
     *
     * @return string
     */
    public function getName()
    {
        return 'Illuminate\Queue\CallQueuedHandler@call';
    }

    /**
     * Get the name of the queued job class.
     *
     * @return string
     */
    public function getJobClass()
    {
        return $this->getJobByQueueName(
            $this->getQueueNameWithoutPrefix(),
            $this->config,
            $this->message->getRoutingKey()
        );
    }

    /**
     * Get the resolved name of the queued job class.
     *
     * Resolves the name of "wrapped" jobs such as class-based handlers.
     *
     * @return string
     */
    public function resolveName()
    {
        return $this->getDisplayName($this->job);
    }

    /**
     * Fire the job.
     *
     * @throws Exception
     *
     * @return void
     */
    public function fire(): void
    {
        /** @var Dispatcher $dispatcher */
        $dispatcher = $this->resolve(Dispatcher::class);

        try {
            $command = $this->setJobInstanceIfNecessary($this->job);
        } catch (ModelNotFoundException $e) {
            $this->handleModelNotFound($e);
            return;
        }

        $dispatcher->dispatchNow(
            $command, $this->resolveHandler($command)
        );

        if (!$this->hasFailed() && !$this->isReleased()) {
            $this->ensureNextJobInChainIsDispatched($command);
        }

        if (!$this->isDeletedOrReleased()) {
            $this->delete();
        }
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts(): int
    {
        // set default job attempts to 1 so that jobs can run without retry
        $defaultAttempts = 1;

        return $this->message->getProperty(self::ATTEMPT_COUNT_HEADERS_KEY, $defaultAttempts);
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody(): string
    {
        return $this->message->getBody();
    }

    /**
     * {@inheritdoc}
     */
    public function delete(): void
    {
        parent::delete();

        if (!$this->isAutoAck()) {
            $this->consumer->acknowledge($this->message);
        }
    }

    /**
     * Process an exception that caused the job to fail.
     *
     * @param  \Exception  $e
     * @return void
     */
    public function failed($e)
    {
        $this->markAsFailed();

        if (method_exists($this->job, 'failed')) {
            $this->job->failed($e);
        }
    }

    /**
     * @return bool
     */
    protected function isAutoAck()
    {
        $queueOptions = $this->config['options']['queue'];
        $queueName = $this->getQueueNameWithoutPrefix();
        return $queueOptions['auto_delete'] ||
            (isset($queueOptions['optionsMapping'][$queueName]['auto_delete']) &&
                $queueOptions['optionsMapping'][$queueName]['auto_delete']);
    }

    /**
     * {@inheritdoc}
     * @throws Exception
     * @throws \Throwable
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        $isAutoAck = $this->isAutoAck();

        if ($delay > 0) {
            if (!$isAutoAck) {
                $this->consumer->acknowledge($this->message);
            }
            $this->connection->later($delay, $this->job, $this->job->data, $this->job->queue);
            return;
        }

        if (!$isAutoAck) {
            $this->consumer->reject($this->message, true);
        } else {
            $this->connection->push($this->job, $this->job->data, $this->job->queue);
        }
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId(): string
    {
        return $this->message->getCorrelationId();
    }

    /**
     * Sets the job identifier.
     *
     * @param string $id
     *
     * @return void
     */
    public function setJobId($id): void
    {
        $this->connection->setCorrelationId($id);
    }
}
